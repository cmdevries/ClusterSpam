/*#include <iostream>
#include <boost/format.hpp>
#include <cstdlib>
#include <unordered_map>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
*/

#include <algorithm>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <inttypes.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/bzip2.hpp>

using namespace std;

hash<string> strhash;

void load_spamscores_clueweb12(const string& directory, unordered_map<size_t, float>& doc2spam) {
  boost::filesystem::path dir(directory);
  boost::filesystem::directory_iterator end;
  for (auto it = boost::filesystem::directory_iterator(dir); it != end; ++it) {
    if (!boost::filesystem::is_regular_file(it->path())) {
      continue;
    }
    cerr << it->path() << endl;
    
    ifstream fin(it->path().string());
    boost::iostreams::filtering_istream in;
    in.push(boost::iostreams::gzip_decompressor());
    in.push(fin);
    string line;
    while (getline(in, line)) {
      vector<string> tokens;
      boost::split(tokens, line, boost::is_any_of(" "));
      float spam_score = boost::lexical_cast<float>(tokens[0]);
      string& docid = tokens[1];
      doc2spam.insert(make_pair(strhash(docid), spam_score));
    }
    cerr << doc2spam.size() << endl;
    
  }
}

void load_spamscores_clueweb09(const string& file, unordered_map<size_t, float>& doc2spam) {
  ifstream in(file.c_str());
  if (!in) {
    cout << "failed to open " << file << endl;
    exit(EXIT_FAILURE);
  } 
    string line;
    while (getline(in, line)) {
      vector<string> tokens;
      boost::split(tokens, line, boost::is_any_of(" "));
      float spam_score = boost::lexical_cast<float>(tokens[0]);
      string& docid = tokens[1];
      doc2spam.insert(make_pair(strhash(docid), spam_score));
    }
}

void load_clusters(const string& cluster_file, unordered_map<size_t, size_t>* doc2cluster, unordered_map<size_t, size_t>* cluster2size) {
    ifstream in(cluster_file);
    string line;
    vector<string> tokens;
    size_t i = 0;
    while (getline(in, line)) {
        tokens.clear();
        boost::split(tokens, line, boost::is_any_of(","));
        if (tokens.size() < 2) {
            cerr << boost::format("%1: can not parse line '%2%'") % cluster_file % line << endl;
            continue;
        }
        string& docid = tokens[0];
        string& clusterid = tokens[1];
        size_t clusterid_hash = strhash(clusterid);
        if (!doc2cluster->insert(make_pair(strhash(docid), clusterid_hash)).second) {
            cerr << boost::format("document %1% has more than one cluster label") % docid << endl;
            continue;
        }
        auto it = cluster2size->find(clusterid_hash);
        if (it == cluster2size->end()) {
            it = cluster2size->insert(it, make_pair(clusterid_hash, 0));
        }
        it->second++;
        if (++i % 10000000 == 0) cerr << i << endl;
    }
}

struct score_t {
    vector<double> spam_score;
    vector<double> size;
};

void score(unordered_map<size_t, size_t>* doc2cluster, unordered_map<size_t, float>* doc2spam, unordered_map<size_t, size_t>* cluster2size) {

    // process all documents 
    unordered_map<size_t, double> cluster2spam;
    for (auto& entry : *doc2spam) {
        const size_t& docid = entry.first;
        const float& spam_score = entry.second;
        const size_t& clusterid = (*doc2cluster)[docid];
        
        // accumulate relevant count in cluster
        auto it = cluster2spam.find(clusterid);
        if (it == cluster2spam.end()) {
          it = cluster2spam.insert(cluster2spam.begin(), make_pair(clusterid, 0));
        }
        it->second += spam_score;
    }

    for (auto& entry : cluster2spam) {
      size_t size = (*cluster2size)[entry.first];
      entry.second /= size;
    }

        // sort by spam score 
        vector<pair<size_t, double>> cluster_spam;
        for (auto& entry : cluster2spam) {
            cluster_spam.push_back(entry);
        }
        sort(cluster_spam.begin(), cluster_spam.end(), [](const pair<size_t, double>& l, const pair<size_t, double>& r) { return l.second > r.second; });

        // collect relevant document counts and cluster sizes
        score_t score;
        for (auto& entry : cluster_spam) {
            score.spam_score.push_back(entry.second);
            size_t& clusterid = entry.first;
            score.size.push_back((*cluster2size)[clusterid]);
        }

        auto convert = [](vector<double>& v, double total) {
            double cumulative_sum = 0;
            transform(v.begin(), v.end(), v.begin(), [&](double d) {
                cumulative_sum += d; 
                return cumulative_sum;
            } );
            transform(v.begin(), v.end(), v.begin(), [&](double d) { return d / total; } );
        };
        double total_size = accumulate(score.size.begin(), score.size.end(), 0);
        convert(score.size, total_size);

        cout << "spam score";
        for (double s : score.spam_score) {
          cout << "," << s;
        }
        cout << endl;

        cout << "percentage of documents";
        for (double p : score.size) {
          cout << "," << p * 100;
        }
        cout << endl;
}

void make_baseline(unordered_map<size_t, size_t>* baseline_doc2cluster, unordered_map<size_t, size_t>* doc2cluster, unordered_map<size_t, size_t>* cluster2size) {
    srand(doc2cluster->size()); // always generate the same random baseline
    vector<size_t> docids;
    docids.reserve(doc2cluster->size());
    baseline_doc2cluster->reserve(doc2cluster->size());
    for (auto& entry : *doc2cluster) {
        docids.push_back(entry.first);
    }
    random_shuffle(docids.begin(), docids.end());
    size_t begin = 0;
    for (auto& entry : *cluster2size) {
        size_t end = begin + entry.second;
        for (size_t i = begin; i < end; i++) {
            baseline_doc2cluster->insert(make_pair(docids[i], entry.first));
        }
        begin = end;
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        cout << boost::format("usage: %1% [in: single lable cluster file] [in: spam file or directory]") % argv[0] << endl;
        return EXIT_FAILURE;
    }

    string spam_path(argv[2]);
    unordered_map<size_t, float> doc2spam;
    doc2spam.reserve(750000000);
    if (boost::filesystem::is_directory(boost::filesystem::path(spam_path))) {
      load_spamscores_clueweb12(spam_path, doc2spam);
    } else {
      load_spamscores_clueweb09(spam_path, doc2spam);
    }

    string cluster_file(argv[1]);
    unordered_map<size_t, size_t> doc2cluster;
    doc2cluster.reserve(doc2spam.size());
    unordered_map<size_t, size_t> cluster2size;
    cluster2size.reserve(doc2spam.size());
    load_clusters(cluster_file, &doc2cluster, &cluster2size);
    cerr << boost::format("loaded %1% documents cluster info") % doc2cluster.size() << endl;
    
    // output total number of documents
    cout << "documents," << doc2cluster.size() << endl;

    unordered_map<size_t, size_t> baseline_doc2cluster;
    baseline_doc2cluster.reserve(doc2spam.size());
    make_baseline(&baseline_doc2cluster, &doc2cluster, &cluster2size);
    
    // output scores for each topic
    cout << "name," << cluster_file << endl;
    score(&doc2cluster, &doc2spam, &cluster2size);
    cout << "name,random baseline" << endl;
    score(&baseline_doc2cluster, &doc2spam, &cluster2size);

    cerr << "submission documents = " << doc2cluster.size() << endl;
    cerr << "baseline documents = " << baseline_doc2cluster.size() << endl;
}
