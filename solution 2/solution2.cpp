#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <pqxx/pqxx>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct Point {
    double x;
    double y;
    
    bool operator<(const Point& other) const {
        if (y != other.y) return y < other.y;
        return x < other.x;
    }
};

struct Region {
    Point p_min;
    Point p_max;
};

struct QueryParams {
    Region region;
    int category = -1; // -1 means not specified
    std::vector<long> one_of_groups;
    bool proper = false;
};

struct InspectionPoint {
    long id;
    long group_id;
    double x;
    double y;
    int category;
};

class RegionQuery {
private:
    std::string connection_string_;
    
public:
    RegionQuery(const std::string& conn_str) : connection_string_(conn_str) {}
    
    bool executeQuery(const std::string& query_file, const std::string& output_file) {
        try {
            // Parse JSON query
            auto query_params = parseQueryFile(query_file);
            if (!query_params.has_value()) {
                return false;
            }
            
            // Execute query against database
            auto points = executeDatabaseQuery(query_params.value());
            if (!points.has_value()) {
                return false;
            }
            
            // Sort points by (y, x)
            std::sort(points->begin(), points->end(), [](const InspectionPoint& a, const InspectionPoint& b) {
                if (a.y != b.y) return a.y < b.y;
                return a.x < b.x;
            });
            
            // Write output file
            return writeOutputFile(output_file, points.value());
            
        } catch (const std::exception& e) {
            std::cerr << "Error executing query: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    std::optional<QueryParams> parseQueryFile(const std::string& query_file) {
        try {
            std::ifstream file(query_file);
            if (!file.is_open()) {
                std::cerr << "Cannot open query file: " << query_file << std::endl;
                return std::nullopt;
            }
            
            json j;
            file >> j;
            
            QueryParams params;
            
            // Parse region from operator_crop
            auto& crop = j["query"]["operator_crop"];
            params.region.p_min.x = crop["region"]["p_min"]["x"];
            params.region.p_min.y = crop["region"]["p_min"]["y"];
            params.region.p_max.x = crop["region"]["p_max"]["x"];
            params.region.p_max.y = crop["region"]["p_max"]["y"];
            
            // Parse optional parameters
            if (crop.contains("category") && !crop["category"].is_null()) {
                params.category = crop["category"];
            }
            
            if (crop.contains("one_of_groups") && !crop["one_of_groups"].is_null()) {
                for (auto& group : crop["one_of_groups"]) {
                    params.one_of_groups.push_back(group);
                }
            }
            
            if (crop.contains("proper") && !crop["proper"].is_null()) {
                params.proper = crop["proper"];
            }
            
            std::cout << "Parsed query: region=(" << params.region.p_min.x << "," << params.region.p_min.y 
                      << ")-(" << params.region.p_max.x << "," << params.region.p_max.y << ")" 
                      << ", category=" << (params.category != -1 ? std::to_string(params.category) : "any")
                      << ", groups_count=" << params.one_of_groups.size()
                      << ", proper=" << (params.proper ? "true" : "false") << std::endl;
            
            return params;
            
        } catch (const std::exception& e) {
            std::cerr << "Error parsing JSON: " << e.what() << std::endl;
            return std::nullopt;
        }
    }
    
    std::optional<std::vector<InspectionPoint>> executeDatabaseQuery(const QueryParams& params) {
        try {
            pqxx::connection conn(connection_string_);
            pqxx::work txn(conn);
            
            std::string query = buildQuery(params);
            std::cout << "Executing query: " << query << std::endl;
            
            auto result = txn.exec(query);
            std::vector<InspectionPoint> points;
            
            for (const auto& row : result) {
                InspectionPoint point;
                point.id = row["id"].as<long>();
                point.group_id = row["group_id"].as<long>();
                point.x = row["coord_x"].as<double>();
                point.y = row["coord_y"].as<double>();
                point.category = row["category"].as<int>();
                points.push_back(point);
            }
            
            std::cout << "Found " << points.size() << " points" << std::endl;
            return points;
            
        } catch (const std::exception& e) {
            std::cerr << "Database query error: " << e.what() << std::endl;
            return std::nullopt;
        }
    }
    
    std::string buildQuery(const QueryParams& params) {
        std::string query = 
            "SELECT ir.id, ir.group_id, ir.coord_x, ir.coord_y, ir.category "
            "FROM inspection_region ir ";
        
        // Add JOIN for proper points check if needed
        if (params.proper) {
            query += 
                "JOIN ("
                "    SELECT group_id "
                "    FROM inspection_region "
                "    GROUP BY group_id "
                "    HAVING "
                "        EVERY(coord_x BETWEEN " + std::to_string(params.region.p_min.x) + 
                " AND " + std::to_string(params.region.p_max.x) + ") AND "
                "        EVERY(coord_y BETWEEN " + std::to_string(params.region.p_min.y) + 
                " AND " + std::to_string(params.region.p_max.y) + ")"
                ") proper_groups ON ir.group_id = proper_groups.group_id ";
        }
        
        query += "WHERE "
                 "ir.coord_x BETWEEN " + std::to_string(params.region.p_min.x) + 
                 " AND " + std::to_string(params.region.p_max.x) + " AND "
                 "ir.coord_y BETWEEN " + std::to_string(params.region.p_min.y) + 
                 " AND " + std::to_string(params.region.p_max.y);
        
        // Add category filter if specified
        if (params.category != -1) {
            query += " AND ir.category = " + std::to_string(params.category);
        }
        
        // Add groups filter if specified
        if (!params.one_of_groups.empty()) {
            query += " AND ir.group_id IN (";
            for (size_t i = 0; i < params.one_of_groups.size(); ++i) {
                if (i > 0) query += ", ";
                query += std::to_string(params.one_of_groups[i]);
            }
            query += ")";
        }
        
        return query;
    }
    
    bool writeOutputFile(const std::string& output_file, const std::vector<InspectionPoint>& points) {
        try {
            std::ofstream file(output_file);
            if (!file.is_open()) {
                std::cerr << "Cannot open output file: " << output_file << std::endl;
                return false;
            }
            
            // Write points in format: x y
            for (const auto& point : points) {
                file << point.x << " " << point.y << std::endl;
            }
            
            std::cout << "Output written to: " << output_file << " with " << points.size() << " points" << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Error writing output: " << e.what() << std::endl;
            return false;
        }
    }
};

int main(int argc, char* argv[]) {
    std::string query_file;
    std::string output_file = "output.txt";
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--query" && i + 1 < argc) {
            query_file = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            output_file = argv[++i];
        }
    }
    
    if (query_file.empty()) {
        std::cerr << "Usage: " << argv[0] << " --query <query_file.json> [--output <output_file.txt>]" << std::endl;
        return 1;
    }
    
    if (!std::filesystem::exists(query_file)) {
        std::cerr << "Query file does not exist: " << query_file << std::endl;
        return 1;
    }
    
    // Database connection - use the same as solution1
    std::string connection_string = "dbname=inspection_db user=kyi host=localhost port=5432";
    
    try {
        RegionQuery query(connection_string);
        
        if (query.executeQuery(query_file, output_file)) {
            std::cout << "Query executed successfully!" << std::endl;
            return 0;
        } else {
            std::cerr << "Query execution failed!" << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}