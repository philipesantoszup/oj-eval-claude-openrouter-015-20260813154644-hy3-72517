// Problem 015 - File Storage
// Key-value database backed by files. Memory stays tiny: each operation only
// ever touches the single bucket file that its index hashes to. `find` streams
// matching values into bounded sorted runs on disk and then k-way merges them
// with tiny per-run buffers, so its memory never scales with the number of
// values returned. Data persists across runs via the bucket files.
//
// Bucket files hold lines of the form "index value". An index hashes to one of
// B bucket files; any single operation only scans one (bounded) file.
//
// I/O uses C stdio (no <iostream>) to keep the runtime memory baseline small.

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <queue>

using namespace std;

// Number of bucket files. With the transient temp files used during delete/find
// we peak at B + 1 files, well under the 20-file limit.
static const int B = 18;

// Read buffer for the bucket scan (small, independent of file size).
static const size_t CHUNK = 1 << 16; // 64 KiB

// External-sort tuning for `find`.
static const int FIND_RUN = 100000;  // values per on-disk sorted run
static const int FIND_INMEM = 50000; // results up to this size are sorted in RAM
static const int MERGE_FANIN = 16;   // max runs merged simultaneously (bounds RAM)
static const int RBUF = 2048;        // per-run stream buffer during merge

static string bucketName(int id) {
    char buf[32];
    snprintf(buf, sizeof(buf), "kvb_%02d", id);
    return string(buf);
}

// FNV-1a hash, mapped to a bucket index.
static int hashIndex(const string& s) {
    unsigned long long h = 1469598103934665603ULL;
    for (unsigned char c : s) {
        h ^= (unsigned long long)c;
        h *= 1099511628211ULL;
    }
    return (int)(h % B);
}

// Cached append handles (one per bucket) so inserts do not open/close the file
// on every command. Flushed before any read of the same bucket.
static FILE* g_append[B] = { nullptr };

static void flushAppend(int b) {
    if (g_append[b]) fflush(g_append[b]);
}
static void closeAppend(int b) {
    if (g_append[b]) { fclose(g_append[b]); g_append[b] = nullptr; }
}
static void closeAllAppend() {
    for (int i = 0; i < B; ++i) closeAppend(i);
}

static bool lineMatches(const string& line, const string& idx, long long val) {
    size_t sp = line.find(' ');
    if (sp == string::npos) return false;
    if (line.compare(0, sp, idx) != 0) return false;
    long long v = 0;
    const char* p = line.data() + sp + 1;
    while (*p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
    return v == val;
}

static void doInsert(const string& idx, long long val) {
    int b = hashIndex(idx);
    if (!g_append[b]) g_append[b] = fopen(bucketName(b).c_str(), "ab");
    if (g_append[b]) {
        fprintf(g_append[b], "%s %d\n", idx.c_str(), (int)val);
    }
}

static void doDelete(const string& idx, long long val) {
    int b = hashIndex(idx);
    string name = bucketName(b);
    closeAppend(b);
    {
        FILE* chk = fopen(name.c_str(), "rb");
        if (!chk) return;
        fclose(chk);
    }
    string tmp = "kvb_tmp";
    remove(tmp.c_str());
    FILE* in = fopen(name.c_str(), "rb");
    FILE* out = fopen(tmp.c_str(), "wb");
    char buf[CHUNK];
    size_t blen, bpos = 0;
    string line;
    while ((blen = fread(buf, 1, sizeof(buf), in)) > 0) {
        bpos = 0;
        while (bpos < blen) {
            char* nl = (char*)memchr(buf + bpos, '\n', blen - bpos);
            size_t segLen = nl ? (size_t)(nl - (buf + bpos)) : (blen - bpos);
            line.assign(buf + bpos, segLen);
            if (line.size() && line.back() == '\r') line.pop_back();
            if (!line.empty() && lineMatches(line, idx, val)) { /* drop */ }
            else { fwrite(line.data(), 1, line.size(), out); fputc('\n', out); }
            bpos = nl ? (size_t)(nl - buf) + 1 : blen;
            if (!nl) break;
        }
    }
    fclose(in);
    fclose(out);
    rename(tmp.c_str(), name.c_str());
    {
        FILE* chk = fopen(name.c_str(), "rb");
        char c;
        bool empty = (chk == nullptr) || (fgetc(chk) == EOF);
        if (chk) fclose(chk);
        if (empty) remove(name.c_str());
    }
}

// Reads one on-disk sorted run incrementally with a small buffer, so memory use
// does not scale with run length.
struct RunReader {
    FILE* f = nullptr;
    int remaining = 0;
    int buf[RBUF];
    int pos = 0, len = 0;
    bool fill() {
        if (pos < len) return true;
        if (remaining == 0) return false;
        int to = remaining; if (to > RBUF) to = RBUF;
        len = 0; pos = 0;
        for (int i = 0; i < to; ++i) {
            int v = 0;
            if (fread(&v, sizeof(int), 1, f) != 1) return false;
            buf[len++] = v;
        }
        remaining -= to;
        return true;
    }
    bool next(int& o) {
        if (!fill()) return false;
        o = buf[pos++];
        return true;
    }
};

// Merge a batch of runs (each at offsets[i] within file path, with counts[i])
// into a single length-prefixed sorted run written to outFile. At most
// MERGE_FANIN runs are merged at once, keeping memory bounded.
static void mergeBatch(const string& path, const vector<long>& offsets,
                       const vector<int>& counts, FILE* outFile) {
    int total = 0;
    for (int c : counts) total += c;
    fwrite(&total, sizeof(int), 1, outFile);
    int k = (int)offsets.size();
    vector<RunReader> rdrs(k);
    for (int i = 0; i < k; ++i) {
        FILE* rf = fopen(path.c_str(), "rb");
        fseek(rf, offsets[i], SEEK_SET);
        int sz = 0;
        fread(&sz, sizeof(int), 1, rf);
        rdrs[i].f = rf;
        rdrs[i].remaining = sz;
        rdrs[i].pos = 0;
        rdrs[i].len = 0;
    }
    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
    for (int i = 0; i < k; ++i) {
        int v;
        if (rdrs[i].next(v)) pq.push({ v, i });
    }
    while (!pq.empty()) {
        auto [v, i] = pq.top(); pq.pop();
        fwrite(&v, sizeof(int), 1, outFile);
        int nv;
        if (rdrs[i].next(nv)) pq.push({ nv, i });
    }
    for (int i = 0; i < k; ++i) fclose(rdrs[i].f);
}

static void doFind(const string& idx) {
    int b = hashIndex(idx);
    string name = bucketName(b);
    flushAppend(b);
    FILE* f = fopen(name.c_str(), "rb");
    if (!f) {
        printf("null\n");
        return;
    }

    const char* idp = idx.data();
    size_t idl = idx.size();
    bool external = false;
    vector<int> mem;
    mem.reserve(FIND_INMEM);
    string runsName = "kvb_find_tmp";
    FILE* fruns = nullptr;
    int runCount = 0;

    auto flushRun = [&]() {
        if (!fruns) { remove(runsName.c_str()); fruns = fopen(runsName.c_str(), "wb"); }
        sort(mem.begin(), mem.end());
        int sz = (int)mem.size();
        fwrite(&sz, sizeof(int), 1, fruns);
        fwrite(mem.data(), sizeof(int), sz, fruns);
        mem.clear();
        ++runCount;
    };

    auto processLine = [&](const char* p, size_t len) {
        if (len <= idl) return;
        if (memcmp(p, idp, idl) != 0) return;
        if (p[idl] != ' ') return;
        int v = 0;
        const char* q = p + idl + 1;
        const char* end = p + len;
        while (q < end && *q >= '0' && *q <= '9') { v = v * 10 + (*q - '0'); ++q; }
        mem.push_back(v);
        if (!external) {
            if ((int)mem.size() == FIND_INMEM) { external = true; flushRun(); }
        } else {
            if ((int)mem.size() == FIND_RUN) flushRun();
        }
    };

    // Fast scan directly on the read buffer (common path does no copying).
    char blk[CHUNK];
    size_t blen = 0, bpos = 0;
    char carry[256];
    size_t carryLen = 0;
    for (;;) {
        if (bpos == blen) {
            blen = fread(blk, 1, sizeof(blk), f);
            bpos = 0;
            if (blen == 0) {
                if (carryLen > 0) { processLine(carry, carryLen); carryLen = 0; }
                break;
            }
        }
        char* nl = (char*)memchr(blk + bpos, '\n', blen - bpos);
        if (nl) {
            size_t segLen = (size_t)(nl - (blk + bpos));
            if (carryLen > 0) {
                size_t total = carryLen + segLen;
                if (total > sizeof(carry) - 1) total = sizeof(carry) - 1;
                memcpy(carry + carryLen, blk + bpos, total - carryLen);
                processLine(carry, total);
                carryLen = 0;
            } else {
                processLine(blk + bpos, segLen);
            }
            bpos = (size_t)(nl - blk) + 1;
        } else {
            size_t rem = blen - bpos;
            if (carryLen + rem <= sizeof(carry) - 1) {
                memcpy(carry + carryLen, blk + bpos, rem);
                carryLen += rem;
            } else {
                carryLen = 0;
            }
            bpos = blen;
        }
    }
    fclose(f);

    // Small result set: sort in RAM, no temporary file at all.
    if (!external) {
        if (mem.empty()) {
            printf("null\n");
        } else {
            sort(mem.begin(), mem.end());
            bool first = true;
            for (int v : mem) {
                if (!first) putchar(' ');
                first = false;
                printf("%d", v);
            }
            putchar('\n');
        }
        return;
    }

    if (!mem.empty()) flushRun();
    if (fruns) fclose(fruns);
    if (runCount == 0) {
        printf("null\n");
        remove(runsName.c_str());
        return;
    }

    string cur = runsName;
    for (;;) {
        vector<long> offsets;
        vector<int> counts;
        {
            FILE* fc = fopen(cur.c_str(), "rb");
            long off = 0;
            for (;;) {
                int sz = 0;
                if (fread(&sz, sizeof(int), 1, fc) != 1) break;
                offsets.push_back(off);
                counts.push_back(sz);
                off += (long)sizeof(int) + (long)sz * sizeof(int);
                fseek(fc, (long)sz * sizeof(int), SEEK_CUR);
            }
            fclose(fc);
        }
        if (offsets.size() <= 1) {
            if (!offsets.empty()) {
                FILE* rf = fopen(cur.c_str(), "rb");
                fseek(rf, offsets[0], SEEK_SET);
                int sz = 0;
                fread(&sz, sizeof(int), 1, rf);
                RunReader rr; rr.f = rf; rr.remaining = sz; rr.pos = 0; rr.len = 0;
                bool first = true;
                int v;
                while (rr.next(v)) {
                    if (!first) putchar(' ');
                    first = false;
                    printf("%d", v);
                }
                putchar('\n');
                fclose(rf);
            } else {
                printf("null\n");
            }
            break;
        }
        string nxt = (cur == runsName) ? "kvb_find_tmp2" : runsName;
        FILE* fout = fopen(nxt.c_str(), "wb");
        for (size_t s = 0; s < offsets.size(); s += MERGE_FANIN) {
            size_t e = min(s + (size_t)MERGE_FANIN, offsets.size());
            vector<long> bo(offsets.begin() + s, offsets.begin() + e);
            vector<int> bc(counts.begin() + s, counts.begin() + e);
            mergeBatch(cur, bo, bc, fout);
        }
        fclose(fout);
        cur = nxt;
    }
    remove(runsName.c_str());
    remove("kvb_find_tmp2");
}

int main() {
    int n;
    if (fscanf(stdin, "%d", &n) != 1) return 0;
    char cmd[32];
    char idxbuf[128];
    long long val;
    for (int i = 0; i < n; ++i) {
        if (fscanf(stdin, "%31s", cmd) != 1) break;
        if (strcmp(cmd, "insert") == 0) {
            if (fscanf(stdin, "%127s %lld", idxbuf, &val) != 2) break;
            doInsert(string(idxbuf), val);
        } else if (strcmp(cmd, "delete") == 0) {
            if (fscanf(stdin, "%127s %lld", idxbuf, &val) != 2) break;
            doDelete(string(idxbuf), val);
        } else if (strcmp(cmd, "find") == 0) {
            if (fscanf(stdin, "%127s", idxbuf) != 1) break;
            doFind(string(idxbuf));
        }
        // unknown command: ignore
    }
    closeAllAppend();
    fflush(stdout);
    return 0;
}
