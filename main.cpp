// Problem 015 - File Storage
// Key-value database backed by files. Memory stays tiny: each operation only
// touches the single bucket file that its index hashes to, and rewrites are
// streamed line-by-line. Data persists across runs via the bucket files.
//
// Bucket files hold lines of the form "index value". An index hashes to one of
// B bucket files, so any single operation only ever scans one (bounded) file.
// A chunked line reader keeps per-operation memory bounded regardless of how
// large a bucket grows.

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cstring>

using namespace std;

// Number of bucket files. Must stay well under the 20-file limit; with the
// transient temp file used during delete we peak at B + 1 files.
static const int B = 18;

// Read buffer for the chunked line scanner (kept small to respect the MiB-scale
// memory budget; independent of file size).
static const size_t CHUNK = 1 << 17; // 128 KiB

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
    // Returns false at end-of-file. Each successful call makes getLine() valid.
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
            if (bufLen == 0) {
                // EOF: emit any trailing line lacking a newline, else stop.
                return !line.empty();
            }
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
    FILE* out = fopen(bucketName(b).c_str(), "ab");
    if (out) {
        fprintf(out, "%s %d\n", idx.c_str(), (int)val);
        fclose(out);
    }
}

static void doDelete(const string& idx, long long val) {
    int b = hashIndex(idx);
    string name = bucketName(b);
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
    // Atomically replace the bucket file with the filtered copy.
    rename(tmp.c_str(), name.c_str());
    // Drop the file entirely if it became empty.
    {
        LineScanner chk;
        if (chk.open(name) && !chk.next()) remove(name.c_str());
    }
}

static void doFind(const string& idx, ostream& os) {
    int b = hashIndex(idx);
    string name = bucketName(b);
    LineScanner in;
    if (!in.open(name)) {
        os << "null\n";
        return;
    }
    vector<int> vals;
    size_t idxLen = idx.size();
    while (in.next()) {
        const string& l = in.getLine();
        if (l.empty()) continue;
        if (l.size() <= idxLen) continue;
        if (l[idxLen] != ' ') continue;
        if (l.compare(0, idxLen, idx) != 0) continue;
        int v = 0;
        const char* p = l.data() + idxLen + 1;
        while (*p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
        vals.push_back(v);
    }
    if (vals.empty()) {
        os << "null\n";
    } else {
        sort(vals.begin(), vals.end());
        for (size_t i = 0; i < vals.size(); ++i) {
            if (i) os << ' ';
            os << vals[i];
        }
        os << '\n';
    }
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
        // unknown command: ignore
    }
    cout.flush();
    return 0;
}
