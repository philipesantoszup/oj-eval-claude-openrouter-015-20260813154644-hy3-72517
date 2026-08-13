// Problem 015 - File Storage
// Key-value database backed by files. Memory stays tiny: each operation only
// ever touches the single bucket file that its index hashes to. `find` streams
// matching values into bounded sorted runs on disk and then k-way merges them
// with tiny per-run buffers, so its memory never scales with the number of
// values returned. Data persists across runs via the bucket files.
//
// Bucket files hold lines of the form "index value". An index hashes to one of
// B bucket files; any single operation only scans one (bounded) file.

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <queue>
#include <cstdio>
#include <cstring>

using namespace std;

// Number of bucket files. With the transient temp files used during delete/find
// we peak at B + 1 files, well under the 20-file limit.
static const int B = 18;

// Read buffer for the chunked line scanner (kept small to respect the MiB-scale
// memory budget; independent of file size).
static const size_t CHUNK = 1 << 17; // 128 KiB

// External-sort tuning for `find`.
static const int FIND_RUN = 45000;   // values per on-disk sorted run
static const int FIND_INMEM = 60000; // results up to this size are sorted in RAM
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

// Chunked, file-backed line scanner. Memory use is bounded by CHUNK plus the
// current line (at most ~80 bytes here), so it never scales with file size.
class LineScanner {
    FILE* f = nullptr;
    char buf[CHUNK];
    size_t bufLen = 0, pos = 0;
    string line;
public:
    bool open(const string& name) {
        f = fopen(name.c_str(), "rb");
        return f != nullptr;
    }
    bool next() {
        line.clear();
        for (;;) {
            if (pos < bufLen) {
                char* nl = (char*)memchr(buf + pos, '\n', bufLen - pos);
                if (nl) {
                    size_t len = (size_t)(nl - (buf + pos));
                    line.append(buf + pos, len);
                    pos = (size_t)(nl - buf) + 1;
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    return true;
                }
                line.append(buf + pos, bufLen - pos);
                pos = bufLen;
            }
            bufLen = fread(buf, 1, sizeof(buf), f);
            pos = 0;
            if (bufLen == 0) return !line.empty();
        }
    }
    const string& getLine() const { return line; }
    void close() { if (f) { fclose(f); f = nullptr; } }
};

// Parse a "index value" line and test whether it matches (idx, val).
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
    closeAppend(b); // invalidate any cached handle for this bucket
    {
        FILE* chk = fopen(name.c_str(), "rb");
        if (!chk) return; // nothing to delete
        fclose(chk);
    }
    string tmp = "kvb_tmp";
    remove(tmp.c_str());
    LineScanner in;
    if (!in.open(name)) return;
    FILE* out = fopen(tmp.c_str(), "wb");
    string line;
    while (in.next()) {
        const string& l = in.getLine();
        if (!l.empty() && lineMatches(l, idx, val)) continue;
        fwrite(l.data(), 1, l.size(), out);
        fputc('\n', out);
    }
    in.close();
    fclose(out);
    rename(tmp.c_str(), name.c_str()); // atomic replace
    {
        LineScanner chk;
        if (chk.open(name) && !chk.next()) remove(name.c_str());
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

static void doFind(const string& idx, ostream& os) {
    int b = hashIndex(idx);
    string name = bucketName(b);
    flushAppend(b); // make sure appended data is visible to the reader
    FILE* f = fopen(name.c_str(), "rb");
    if (!f) {
        os << "null\n";
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

    // Fast scan: operate directly on the read buffer. A line only ever spans a
    // block boundary with negligible probability, so the common path does no
    // copying at all.
    char blk[CHUNK];
    size_t blen = 0, bpos = 0;
    char carry[256];
    size_t carryLen = 0;

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
                carryLen = 0; // line absurdly long; skip it
            }
            bpos = blen;
        }
    }
    fclose(f);

    // Small result set: sort in RAM, no temporary file at all.
    if (!external) {
        if (mem.empty()) {
            os << "null\n";
        } else {
            sort(mem.begin(), mem.end());
            bool first = true;
            for (int v : mem) {
                if (!first) os << ' ';
                first = false;
                os << v;
            }
            os << '\n';
        }
        return;
    }

    // Large result set: keep streaming into bounded sorted runs, then merge.
    if (!mem.empty()) flushRun();
    if (fruns) fclose(fruns);
    if (runCount == 0) {
        os << "null\n";
        remove(runsName.c_str());
        return;
    }

    // Discover run boundaries, then k-way merge all runs with tiny buffers.
    vector<long> offsets(runCount);
    {
        FILE* fc = fopen(runsName.c_str(), "rb");
        long off = 0;
        for (int i = 0; i < runCount; ++i) {
            offsets[i] = off;
            int sz = 0;
            fread(&sz, sizeof(int), 1, fc);
            off += (long)sizeof(int) + (long)sz * sizeof(int);
            fseek(fc, (long)sz * sizeof(int), SEEK_CUR);
        }
        fclose(fc);
    }

    vector<RunReader> rdrs(runCount);
    for (int i = 0; i < runCount; ++i) {
        FILE* rf = fopen(runsName.c_str(), "rb");
        fseek(rf, offsets[i], SEEK_SET);
        int sz = 0;
        fread(&sz, sizeof(int), 1, rf);
        rdrs[i].f = rf;
        rdrs[i].remaining = sz;
        rdrs[i].pos = 0;
        rdrs[i].len = 0;
    }

    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
    for (int i = 0; i < runCount; ++i) {
        int v;
        if (rdrs[i].next(v)) pq.push({ v, i });
    }
    bool first = true;
    while (!pq.empty()) {
        auto [v, i] = pq.top(); pq.pop();
        if (!first) os << ' ';
        first = false;
        os << v;
        int nv;
        if (rdrs[i].next(nv)) pq.push({ nv, i });
    }
    os << '\n';

    for (int i = 0; i < runCount; ++i) fclose(rdrs[i].f);
    remove(runsName.c_str());
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);
    int n;
    if (!(cin >> n)) return 0;
    string cmd;
    for (int i = 0; i < n; ++i) {
        if (!(cin >> cmd)) break;
        if (cmd == "insert") {
            string idx; long long val;
            if (!(cin >> idx >> val)) break;
            doInsert(idx, val);
        } else if (cmd == "delete") {
            string idx; long long val;
            if (!(cin >> idx >> val)) break;
            doDelete(idx, val);
        } else if (cmd == "find") {
            string idx;
            if (!(cin >> idx)) break;
            doFind(idx, cout);
        }
    }
    closeAllAppend();
    cout.flush();
    return 0;
}
