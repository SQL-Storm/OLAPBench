#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <initializer_list>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sodium.h>
#include <yaml-cpp/yaml.h>

namespace {

constexpr const char* kWorkerEnvVar = "STRINGIFY_MAX_WORKERS";

struct StringifyRule {
  int index = -1;
  std::string prefix;
  int pad_width = 0;
  int amplification_extra_pad = 0;
  std::string amplification_separator = "_";
  std::string amplification_marker = "X";
};

struct NullRule {
  int index = -1;
  double probability = 0.0;
  std::string name;
};

struct MCVRule {
  int index = -1;
  std::string name;
  double f20 = 0.0;
  double f1 = 0.0;
  std::vector<std::string> values;
};

struct RuleSet {
  bool stringify_enabled = false;
  bool nulls_enabled = false;
  bool mcv_enabled = false;
  int null_seed = 0;
  int mcv_seed = 0;
  std::string null_marker;
  std::unordered_map<std::string, std::vector<StringifyRule>> stringify_rules;
  std::unordered_map<std::string, std::vector<NullRule>> null_rules;
  std::unordered_map<std::string, std::vector<MCVRule>> mcv_rules;
};

struct RewriteTask {
  std::filesystem::path path;
  std::string table;
};

struct FileResult {
  bool rewritten = false;
  uint64_t rows = 0;
};

struct RewriteStats {
  uint64_t files_rewritten = 0;
  uint64_t rows_rewritten = 0;
  double duration_s = 0.0;
};

static void print_usage(const char* prog) {
  std::fprintf(
      stderr,
      "Usage: %s --output-dir <path> --rules-file <path> [--max-workers N] "
      "[--summary-json <path>] [--progress]\n",
      prog);
}

static void ensure_sodium_ready() {
  static std::once_flag once;
  std::call_once(once, []() {
    if (sodium_init() < 0) {
      throw std::runtime_error("sodium_init failed");
    }
  });
}

static uint64_t blake2b_u64_from_parts(
    const std::string& seed,
    std::initializer_list<std::string_view> parts) {
  ensure_sodium_ready();
  crypto_generichash_blake2b_state state;
  if (crypto_generichash_blake2b_init(&state, nullptr, 0, 8) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_init failed");
  }

  if (crypto_generichash_blake2b_update(
          &state,
          reinterpret_cast<const unsigned char*>(seed.data()),
          seed.size()) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_update seed failed");
  }

  const unsigned char sep = 0x1f;
  for (std::string_view part : parts) {
    if (crypto_generichash_blake2b_update(&state, &sep, 1) != 0) {
      throw std::runtime_error("crypto_generichash_blake2b_update sep failed");
    }
    if (crypto_generichash_blake2b_update(
            &state,
            reinterpret_cast<const unsigned char*>(part.data()),
            part.size()) != 0) {
      throw std::runtime_error("crypto_generichash_blake2b_update part failed");
    }
  }

  unsigned char out[8];
  if (crypto_generichash_blake2b_final(&state, out, sizeof(out)) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_final failed");
  }

  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<uint64_t>(out[i]);
  }
  return value;
}

static uint64_t blake2b_u64_from_vec(
    const std::string& seed,
    const std::vector<std::string>& parts) {
  ensure_sodium_ready();
  crypto_generichash_blake2b_state state;
  if (crypto_generichash_blake2b_init(&state, nullptr, 0, 8) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_init failed");
  }
  if (crypto_generichash_blake2b_update(
          &state,
          reinterpret_cast<const unsigned char*>(seed.data()),
          seed.size()) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_update seed failed");
  }
  const unsigned char sep = 0x1f;
  for (const std::string& part : parts) {
    if (crypto_generichash_blake2b_update(&state, &sep, 1) != 0) {
      throw std::runtime_error("crypto_generichash_blake2b_update sep failed");
    }
    if (crypto_generichash_blake2b_update(
            &state,
            reinterpret_cast<const unsigned char*>(part.data()),
            part.size()) != 0) {
      throw std::runtime_error("crypto_generichash_blake2b_update part failed");
    }
  }

  unsigned char out[8];
  if (crypto_generichash_blake2b_final(&state, out, sizeof(out)) != 0) {
    throw std::runtime_error("crypto_generichash_blake2b_final failed");
  }

  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<uint64_t>(out[i]);
  }
  return value;
}

static double stable_unit_hash(
    const std::string& seed,
    std::initializer_list<std::string_view> parts) {
  uint64_t value = blake2b_u64_from_parts(seed, parts);
  constexpr long double denom = 18446744073709551616.0L;  // 2**64
  return static_cast<long double>(value) / denom;
}

static std::vector<std::string> split_row(const std::string& line) {
  std::vector<std::string> parts;
  std::string current;
  current.reserve(32);
  for (char ch : line) {
    if (ch == '|') {
      parts.emplace_back(current);
      current.clear();
    } else {
      current.push_back(ch);
    }
  }
  parts.emplace_back(current);
  return parts;
}

static std::string table_name_from_filename(const std::string& filename) {
  std::string name = filename;
  const auto dot = name.find('.');
  if (dot != std::string::npos) {
    name = name.substr(0, dot);
  }
  std::vector<std::string> parts;
  std::string token;
  std::stringstream ss(name);
  while (std::getline(ss, token, '_')) {
    parts.push_back(token);
  }
  while (!parts.empty()) {
    const std::string& last = parts.back();
    if (!last.empty() && std::all_of(last.begin(), last.end(), ::isdigit)) {
      parts.pop_back();
    } else {
      break;
    }
  }
  if (parts.empty()) {
    return "";
  }
  std::ostringstream out;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i) {
      out << "_";
    }
    out << parts[i];
  }
  return out.str();
}

static RuleSet load_rules(const std::string& path) {
  RuleSet rules;
  YAML::Node root = YAML::LoadFile(path);

  const YAML::Node stringify = root["stringify"];
  if (stringify) {
    rules.stringify_enabled =
        stringify["enabled"] ? stringify["enabled"].as<bool>() : false;
    const YAML::Node map = stringify["rules"];
    if (map) {
      for (auto it = map.begin(); it != map.end(); ++it) {
        const std::string table = it->first.as<std::string>();
        const YAML::Node list = it->second;
        std::vector<StringifyRule> vec;
        vec.reserve(list.size());
        for (const auto& item : list) {
          StringifyRule rule;
          rule.index = item["index"].as<int>();
          rule.prefix = item["prefix"] ? item["prefix"].as<std::string>() : "";
          rule.pad_width = item["pad_width"] ? item["pad_width"].as<int>() : 0;
          rule.amplification_extra_pad =
              item["amplification_extra_pad"] ? item["amplification_extra_pad"].as<int>() : 0;
          rule.amplification_separator = item["amplification_separator"]
              ? item["amplification_separator"].as<std::string>()
              : "_";
          rule.amplification_marker = item["amplification_marker"]
              ? item["amplification_marker"].as<std::string>()
              : "X";
          vec.push_back(std::move(rule));
        }
        rules.stringify_rules[table] = std::move(vec);
      }
    }
  }

  const YAML::Node nulls = root["nulls"];
  if (nulls) {
    rules.nulls_enabled = nulls["enabled"] ? nulls["enabled"].as<bool>() : false;
    rules.null_seed = nulls["seed"] ? nulls["seed"].as<int>() : 0;
    rules.null_marker =
        nulls["null_marker"] ? nulls["null_marker"].as<std::string>() : "";
    const YAML::Node map = nulls["rules"];
    if (map) {
      for (auto it = map.begin(); it != map.end(); ++it) {
        const std::string table = it->first.as<std::string>();
        const YAML::Node list = it->second;
        std::vector<NullRule> vec;
        vec.reserve(list.size());
        for (const auto& item : list) {
          NullRule rule;
          rule.index = item["index"].as<int>();
          rule.probability = item["probability"].as<double>();
          rule.name = item["name"] ? item["name"].as<std::string>() : "";
          vec.push_back(std::move(rule));
        }
        rules.null_rules[table] = std::move(vec);
      }
    }
  }

  const YAML::Node mcv = root["mcv"];
  if (mcv) {
    rules.mcv_enabled = mcv["enabled"] ? mcv["enabled"].as<bool>() : false;
    rules.mcv_seed = mcv["seed"] ? mcv["seed"].as<int>() : 0;
    if (rules.null_marker.empty() && mcv["null_marker"]) {
      rules.null_marker = mcv["null_marker"].as<std::string>();
    }
    const YAML::Node map = mcv["rules"];
    if (map) {
      for (auto it = map.begin(); it != map.end(); ++it) {
        const std::string table = it->first.as<std::string>();
        const YAML::Node list = it->second;
        std::vector<MCVRule> vec;
        vec.reserve(list.size());
        for (const auto& item : list) {
          MCVRule rule;
          rule.index = item["index"].as<int>();
          rule.name = item["name"] ? item["name"].as<std::string>() : "";
          rule.f20 = item["f20"].as<double>();
          rule.f1 = item["f1"].as<double>();
          const YAML::Node values = item["values"];
          if (values) {
            rule.values.reserve(values.size());
            for (const auto& val : values) {
              rule.values.push_back(val.as<std::string>());
            }
          }
          vec.push_back(std::move(rule));
        }
        rules.mcv_rules[table] = std::move(vec);
      }
    }
  }

  return rules;
}

static void apply_nulls(
    std::vector<std::string>& row,
    const std::vector<NullRule>& rules,
    const std::string& seed,
    const std::string& table,
    const std::string& token,
    const std::string& row_index_token,
    const std::string& null_marker) {
  for (const NullRule& rule : rules) {
    if (rule.index < 0 || static_cast<size_t>(rule.index) >= row.size()) {
      continue;
    }
    const std::string& current = row[rule.index];
    if (current.empty() || current == null_marker || current == "\\N") {
      continue;
    }
    const double h = stable_unit_hash(
        seed, {table, rule.name, token, row_index_token});
    if (h < rule.probability) {
      row[rule.index] = null_marker;
    }
  }
}

static void apply_stringify(
    std::vector<std::string>& row,
    const std::vector<StringifyRule>& rules) {
  for (const StringifyRule& rule : rules) {
    if (rule.index < 0 || static_cast<size_t>(rule.index) >= row.size()) {
      continue;
    }
    const std::string& raw = row[rule.index];
    if (raw.empty() || raw == "\\N") {
      continue;
    }
    try {
      long long value = std::stoll(raw);
      std::ostringstream out;
      out << rule.prefix << std::setw(rule.pad_width) << std::setfill('0') << value;
      if (rule.amplification_extra_pad > 0) {
        const std::string separator =
            rule.amplification_separator.empty() ? "_" : rule.amplification_separator;
        const std::string marker =
            rule.amplification_marker.empty() ? "X" : rule.amplification_marker;
        out << separator;
        for (int i = 0; i < rule.amplification_extra_pad; ++i) {
          out << marker;
        }
      }
      row[rule.index] = out.str();
    } catch (...) {
      continue;
    }
  }
}

static void apply_mcv(
    std::vector<std::string>& row,
    const std::vector<MCVRule>& rules,
    const std::string& seed,
    const std::string& table,
    const std::string& token,
    const std::string& row_index_token,
    const std::string& null_marker) {
  for (const MCVRule& rule : rules) {
    if (rule.index < 0 || static_cast<size_t>(rule.index) >= row.size()) {
      continue;
    }
    const std::string& current = row[rule.index];
    if (current.empty() || current == null_marker || current == "\\N") {
      continue;
    }
    if (rule.values.empty()) {
      continue;
    }

    const double h = stable_unit_hash(
        seed, {table, rule.name, token, row_index_token});
    if (h < rule.f1) {
      row[rule.index] = rule.values[0];
    } else if (h < rule.f20 && rule.values.size() > 1) {
      const double pick_hash = stable_unit_hash(
          seed, {table, rule.name, token, "mcv", row_index_token});
      size_t choice = 1 + static_cast<size_t>(pick_hash * (rule.values.size() - 1));
      if (choice >= rule.values.size()) {
        choice = rule.values.size() - 1;
      }
      row[rule.index] = rule.values[choice];
    }
  }
}

static FileResult process_file(
    const std::filesystem::path& path,
    const std::string& table,
    const RuleSet& rules) {
  const bool has_stringify =
      rules.stringify_enabled && rules.stringify_rules.count(table) > 0;
  const bool has_nulls =
      rules.nulls_enabled && rules.null_rules.count(table) > 0;
  const bool has_mcv = rules.mcv_enabled && rules.mcv_rules.count(table) > 0;
  if (!has_stringify && !has_nulls && !has_mcv) {
    return FileResult{};
  }

  std::filesystem::path tmp = path;
  tmp += ".tmp";

  std::ifstream src(path, std::ios::binary);
  if (!src.is_open()) {
    throw std::runtime_error("Failed to open input file: " + path.string());
  }
  std::ofstream dst(tmp, std::ios::binary);
  if (!dst.is_open()) {
    throw std::runtime_error("Failed to open output file: " + tmp.string());
  }

  const std::string null_seed = std::to_string(rules.null_seed);
  const std::string mcv_seed = std::to_string(rules.mcv_seed);
  const std::string token = path.filename().string();

  uint64_t row_index = 0;
  std::string line;
  try {
    while (std::getline(src, line)) {
      if (line.empty()) {
        dst << "\n";
        row_index += 1;
        continue;
      }
      std::vector<std::string> row = split_row(line);
      const std::string row_token = std::to_string(row_index);

      if (has_nulls) {
        apply_nulls(
            row,
            rules.null_rules.at(table),
            null_seed,
            table,
            token,
            row_token,
            rules.null_marker);
      }
      if (has_stringify) {
        apply_stringify(row, rules.stringify_rules.at(table));
      }
      if (has_mcv) {
        apply_mcv(
            row,
            rules.mcv_rules.at(table),
            mcv_seed,
            table,
            token,
            row_token,
            rules.null_marker);
      }

      for (size_t i = 0; i < row.size(); ++i) {
        if (i) {
          dst << "|";
        }
        dst << row[i];
      }
      dst << "\n";
      row_index += 1;
    }
  } catch (...) {
    dst.close();
    src.close();
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    throw;
  }

  dst.close();
  src.close();
  std::filesystem::rename(tmp, path);
  return FileResult{true, row_index};
}

static bool table_has_rules(const RuleSet& rules, const std::string& table) {
  const bool has_stringify =
      rules.stringify_enabled && rules.stringify_rules.count(table) > 0;
  const bool has_nulls =
      rules.nulls_enabled && rules.null_rules.count(table) > 0;
  const bool has_mcv = rules.mcv_enabled && rules.mcv_rules.count(table) > 0;
  return has_stringify || has_nulls || has_mcv;
}

static int resolve_worker_count(int requested, size_t task_count) {
  int worker_target = requested;
  if (worker_target <= 0) {
    const char* env = std::getenv(kWorkerEnvVar);
    if (env) {
      worker_target = std::atoi(env);
    }
  }
  if (worker_target <= 0) {
    worker_target = static_cast<int>(std::thread::hardware_concurrency());
  }
  if (worker_target <= 0) {
    worker_target = 1;
  }
  if (task_count > 0 && worker_target > static_cast<int>(task_count)) {
    worker_target = static_cast<int>(task_count);
  }
  return std::max(1, worker_target);
}

static void write_summary_json(
    const std::filesystem::path& path,
    const RewriteStats& stats) {
  std::ofstream out(path, std::ios::binary);
  if (!out.is_open()) {
    throw std::runtime_error("Failed to open summary json for writing: " + path.string());
  }
  out << "{\n";
  out << "  \"files_rewritten\": " << stats.files_rewritten << ",\n";
  out << "  \"rows_rewritten\": " << stats.rows_rewritten << ",\n";
  out << "  \"duration_s\": " << std::fixed << std::setprecision(6) << stats.duration_s << "\n";
  out << "}\n";
}

static int run_print_hash(int argc, char** argv) {
  if (argc < 3) {
    std::fprintf(stderr, "Missing seed for --print-hash.\n");
    return 2;
  }
  const std::string seed = argv[2];
  std::vector<std::string> parts;
  for (int i = 3; i < argc; ++i) {
    parts.emplace_back(argv[i]);
  }

  try {
    const uint64_t value = blake2b_u64_from_vec(seed, parts);
    std::cout << value << "\n";
    return 0;
  } catch (const std::exception& exc) {
    std::fprintf(stderr, "hash error: %s\n", exc.what());
    return 1;
  }
}

static int run_rewrite(int argc, char** argv) {
  std::string output_dir;
  std::string rules_file;
  std::string summary_json;
  int max_workers = 0;
  bool progress = false;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--output-dir" && i + 1 < argc) {
      output_dir = argv[++i];
    } else if (arg == "--rules-file" && i + 1 < argc) {
      rules_file = argv[++i];
    } else if (arg == "--max-workers" && i + 1 < argc) {
      max_workers = std::stoi(argv[++i]);
    } else if (arg == "--summary-json" && i + 1 < argc) {
      summary_json = argv[++i];
    } else if (arg == "--progress") {
      progress = true;
    }
  }

  if (output_dir.empty() || rules_file.empty()) {
    std::fprintf(stderr, "Missing required --output-dir or --rules-file.\n");
    return 2;
  }

  try {
    const RuleSet rules = load_rules(rules_file);

    std::vector<RewriteTask> tasks;
    for (const auto& entry : std::filesystem::directory_iterator(output_dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      const auto ext = entry.path().extension().string();
      if (ext != ".tbl" && ext != ".dat") {
        continue;
      }
      const std::string table = table_name_from_filename(entry.path().filename().string());
      if (table.empty() || !table_has_rules(rules, table)) {
        continue;
      }
      tasks.push_back(RewriteTask{entry.path(), table});
    }

    RewriteStats stats;
    if (tasks.empty()) {
      if (!summary_json.empty()) {
        write_summary_json(summary_json, stats);
      }
      return 0;
    }

    const int worker_target = resolve_worker_count(max_workers, tasks.size());
    if (progress) {
      std::fprintf(
          stderr,
          "[stringify_cpp] rewrite_start files=%zu workers=%d\n",
          tasks.size(),
          worker_target);
    }

    const auto start = std::chrono::steady_clock::now();
    std::atomic<size_t> next_idx{0};
    std::atomic<uint64_t> files_rewritten{0};
    std::atomic<uint64_t> rows_rewritten{0};
    std::atomic<size_t> files_done{0};
    std::atomic<bool> failed{false};
    std::string first_error;
    std::mutex error_mu;

    std::vector<std::thread> threads;
    threads.reserve(worker_target);
    for (int t = 0; t < worker_target; ++t) {
      threads.emplace_back([&]() {
        while (true) {
          if (failed.load(std::memory_order_relaxed)) {
            break;
          }
          const size_t idx = next_idx.fetch_add(1, std::memory_order_relaxed);
          if (idx >= tasks.size()) {
            break;
          }
          const RewriteTask& task = tasks[idx];
          try {
            const FileResult result = process_file(task.path, task.table, rules);
            if (result.rewritten) {
              files_rewritten.fetch_add(1, std::memory_order_relaxed);
              rows_rewritten.fetch_add(result.rows, std::memory_order_relaxed);
            }
          } catch (const std::exception& exc) {
            bool expected = false;
            if (failed.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
              std::lock_guard<std::mutex> lock(error_mu);
              first_error = exc.what();
            }
            break;
          }

          const size_t done = files_done.fetch_add(1, std::memory_order_relaxed) + 1;
          if (progress && (done == tasks.size() || done % 5 == 0)) {
            std::fprintf(
                stderr,
                "[stringify_cpp] progress %zu/%zu files\n",
                done,
                tasks.size());
          }
        }
      });
    }

    for (std::thread& thread : threads) {
      thread.join();
    }

    if (failed.load(std::memory_order_relaxed)) {
      if (first_error.empty()) {
        first_error = "unknown worker failure";
      }
      std::fprintf(stderr, "[stringify_cpp] ERROR: %s\n", first_error.c_str());
      return 1;
    }

    const auto end = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end - start;
    stats.files_rewritten = files_rewritten.load(std::memory_order_relaxed);
    stats.rows_rewritten = rows_rewritten.load(std::memory_order_relaxed);
    stats.duration_s = elapsed.count();

    if (progress) {
      std::fprintf(
          stderr,
          "[stringify_cpp] rewrite_done files=%llu rows=%llu duration_s=%.3f\n",
          static_cast<unsigned long long>(stats.files_rewritten),
          static_cast<unsigned long long>(stats.rows_rewritten),
          stats.duration_s);
    }

    if (!summary_json.empty()) {
      write_summary_json(summary_json, stats);
    }

    return 0;
  } catch (const std::exception& exc) {
    std::fprintf(stderr, "[stringify_cpp] fatal: %s\n", exc.what());
    return 1;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc >= 2) {
    const std::string arg1 = argv[1];
    if (arg1 == "--print-hash") {
      return run_print_hash(argc, argv);
    }
    if (arg1 == "--help" || arg1 == "-h") {
      print_usage(argv[0]);
      return 0;
    }
  }
  return run_rewrite(argc, argv);
}
