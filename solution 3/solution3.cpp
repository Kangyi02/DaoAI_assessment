#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <set>
#include <pqxx/pqxx>

// Simple JSON parser for our specific format
class SimpleJsonParser {
public:
    struct Region {
        double p_min_x, p_min_y, p_max_x, p_max_y;
    };
    
    struct CropParams {
        Region region;
        int category;
        std::vector<long> one_of_groups;
        bool proper;
        bool has_category;
        bool has_one_of_groups;
        
        CropParams() : category(-1), proper(false), has_category(false), has_one_of_groups(false) {}
    };
    
    // Base class for all query operations
    struct QueryOperation {
        virtual ~QueryOperation() = default;
    };
    
    // Crop operation
    struct CropOperation : QueryOperation {
        CropParams params;
    };
    
    // And operation
    struct AndOperation : QueryOperation {
        std::vector<std::shared_ptr<QueryOperation>> operands;
    };
    
    // Or operation  
    struct OrOperation : QueryOperation {
        std::vector<std::shared_ptr<QueryOperation>> operands;
    };
    
    static std::shared_ptr<QueryOperation> parseQueryFile(const std::string& filename) {
        std::ifstream file(filename);
        std::string line;
        std::string content;
        
        while (std::getline(file, line)) {
            content += line;
        }
        
        // Find the main query object
        size_t query_start = content.find("\"query\":");
        if (query_start == std::string::npos) {
            throw std::runtime_error("No query found in JSON");
        }
        
        return parseOperation(content, query_start + 8); // Skip "\"query\":"
    }
    
private:
    static std::shared_ptr<QueryOperation> parseOperation(const std::string& content, size_t start_pos) {
        // Find which operator we have
        if (content.find("\"operator_crop\"", start_pos) != std::string::npos) {
            return parseCropOperation(content, start_pos);
        } else if (content.find("\"operator_and\"", start_pos) != std::string::npos) {
            return parseAndOperation(content, start_pos);
        } else if (content.find("\"operator_or\"", start_pos) != std::string::npos) {
            return parseOrOperation(content, start_pos);
        } else {
            throw std::runtime_error("Unknown operator in query");
        }
    }
    
    static std::shared_ptr<CropOperation> parseCropOperation(const std::string& content, size_t start_pos) {
        auto crop_op = std::make_shared<CropOperation>();
        
        size_t pos = content.find("\"operator_crop\"", start_pos) + 15;
        
        // Extract region coordinates
        crop_op->params.region.p_min_x = extractDouble(content, "\"p_min\": { \"x\":", pos);
        crop_op->params.region.p_min_y = extractDouble(content, "\"y\":", pos);
        crop_op->params.region.p_max_x = extractDouble(content, "\"p_max\": { \"x\":", pos);
        crop_op->params.region.p_max_y = extractDouble(content, "\"y\":", pos);
        
        // Extract category if present
        size_t category_pos = content.find("\"category\":", pos);
        if (category_pos != std::string::npos && category_pos < content.find("}", pos)) {
            crop_op->params.has_category = true;
            size_t start = content.find(":", category_pos) + 1;
            size_t end = content.find_first_of(",}", start);
            std::string cat_str = content.substr(start, end - start);
            crop_op->params.category = std::stoi(cat_str);
        }
        
        // Extract one_of_groups if present
        size_t groups_pos = content.find("\"one_of_groups\":", pos);
        if (groups_pos != std::string::npos && groups_pos < content.find("}", pos)) {
            crop_op->params.has_one_of_groups = true;
            size_t start = content.find("[", groups_pos) + 1;
            size_t end = content.find("]", start);
            std::string groups_str = content.substr(start, end - start);
            
            // Parse comma-separated group IDs
            size_t group_start = 0;
            while (group_start < groups_str.length()) {
                size_t group_end = groups_str.find(",", group_start);
                if (group_end == std::string::npos) group_end = groups_str.length();
                
                std::string group_str = groups_str.substr(group_start, group_end - group_start);
                if (!group_str.empty()) {
                    crop_op->params.one_of_groups.push_back(std::stol(group_str));
                }
                
                group_start = group_end + 1;
            }
        }
        
        // Extract proper flag if present
        size_t proper_pos = content.find("\"proper\":", pos);
        if (proper_pos != std::string::npos && proper_pos < content.find("}", pos)) {
            size_t start = proper_pos + 9; // length of "\"proper\":"
            std::string proper_str = content.substr(start, 4); // "true" or "fals"
            crop_op->params.proper = (proper_str.find("true") != std::string::npos);
        }
        
        return crop_op;
    }
    
    static std::shared_ptr<AndOperation> parseAndOperation(const std::string& content, size_t start_pos) {
        auto and_op = std::make_shared<AndOperation>();
        
        size_t array_start = content.find("[", start_pos);
        size_t array_end = content.find("]", array_start);
        size_t pos = array_start + 1;
        
        while (pos < array_end) {
            // Skip whitespace and commas
            while (pos < array_end && (content[pos] == ' ' || content[pos] == ',' || content[pos] == '\n')) {
                pos++;
            }
            if (pos >= array_end) break;
            
            // Parse nested operation
            if (content[pos] == '{') {
                and_op->operands.push_back(parseOperation(content, pos));
                // Move position past this object
                pos = findMatchingBrace(content, pos) + 1;
            }
        }
        
        return and_op;
    }
    
    static std::shared_ptr<OrOperation> parseOrOperation(const std::string& content, size_t start_pos) {
        auto or_op = std::make_shared<OrOperation>();
        
        size_t array_start = content.find("[", start_pos);
        size_t array_end = content.find("]", array_start);
        size_t pos = array_start + 1;
        
        while (pos < array_end) {
            // Skip whitespace and commas
            while (pos < array_end && (content[pos] == ' ' || content[pos] == ',' || content[pos] == '\n')) {
                pos++;
            }
            if (pos >= array_end) break;
            
            // Parse nested operation
            if (content[pos] == '{') {
                or_op->operands.push_back(parseOperation(content, pos));
                // Move position past this object
                pos = findMatchingBrace(content, pos) + 1;
            }
        }
        
        return or_op;
    }
    
    static double extractDouble(const std::string& content, const std::string& key, size_t& pos) {
        size_t found = content.find(key, pos);
        if (found == std::string::npos) return 0.0;
        
        size_t start = found + key.length();
        size_t end = content.find_first_of(",}", start);
        std::string num_str = content.substr(start, end - start);
        pos = end;
        
        return std::stod(num_str);
    }
    
    static size_t findMatchingBrace(const std::string& content, size_t start) {
        int brace_count = 0;
        for (size_t i = start; i < content.length(); ++i) {
            if (content[i] == '{') brace_count++;
            else if (content[i] == '}') brace_count--;
            
            if (brace_count == 0) return i;
        }
        return std::string::npos;
    }
};

struct InspectionPoint {
    long id;
    long group_id;
    double x;
    double y;
    int category;
    
    // For sorting: by y then x
    bool operator<(const InspectionPoint& other) const {
        if (y < other.y) return true;
        if (y > other.y) return false;
        return x < other.x;
    }
    
    // For set operations
    bool operator==(const InspectionPoint& other) const {
        return id == other.id;
    }
};

// Hash function for InspectionPoint (for set operations)
struct InspectionPointHash {
    std::size_t operator()(const InspectionPoint& p) const {
        return std::hash<long>()(p.id);
    }
};

class RegionQuery {
private:
    std::string connection_string_;
    
public:
    RegionQuery(const std::string& conn_str) : connection_string_(conn_str) {}
    
    bool executeQuery(const std::string& query_file, const std::string& output_file) {
        try {
            // Parse JSON query
            auto query_op = SimpleJsonParser::parseQueryFile(query_file);
            
            // Execute query against database
            auto points = executeOperation(query_op);
            
            // Sort points by (y, x)
            std::sort(points.begin(), points.end());
            
            // Write output file
            return writeOutputFile(output_file, points);
            
        } catch (const std::exception& e) {
            std::cerr << "Error executing query: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    std::vector<InspectionPoint> executeOperation(const std::shared_ptr<SimpleJsonParser::QueryOperation>& op) {
        if (auto crop_op = std::dynamic_pointer_cast<SimpleJsonParser::CropOperation>(op)) {
            return executeCropOperation(*crop_op);
        } else if (auto and_op = std::dynamic_pointer_cast<SimpleJsonParser::AndOperation>(op)) {
            return executeAndOperation(*and_op);
        } else if (auto or_op = std::dynamic_pointer_cast<SimpleJsonParser::OrOperation>(op)) {
            return executeOrOperation(*or_op);
        } else {
            throw std::runtime_error("Unknown operation type");
        }
    }
    
    std::vector<InspectionPoint> executeCropOperation(const SimpleJsonParser::CropOperation& op) {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        std::string query = buildCropQuery(op.params);
        std::cout << "Executing crop query: " << query << std::endl;
        
        auto result = txn.exec(query);
        std::vector<InspectionPoint> points;
        
        for (size_t i = 0; i < result.size(); ++i) {
            InspectionPoint point;
            point.id = result[i]["id"].as<long>();
            point.group_id = result[i]["group_id"].as<long>();
            point.x = result[i]["coord_x"].as<double>();
            point.y = result[i]["coord_y"].as<double>();
            point.category = result[i]["category"].as<int>();
            points.push_back(point);
        }
        
        std::cout << "Found " << points.size() << " points" << std::endl;
        return points;
    }
    
    std::vector<InspectionPoint> executeAndOperation(const SimpleJsonParser::AndOperation& op) {
        if (op.operands.empty()) {
            return {};
        }
        
        // Start with first operand
        auto result_set = executeOperation(op.operands[0]);
        std::set<long> result_ids;
        for (const auto& point : result_set) {
            result_ids.insert(point.id);
        }
        
        // Intersect with remaining operands
        for (size_t i = 1; i < op.operands.size(); ++i) {
            auto current_set = executeOperation(op.operands[i]);
            std::set<long> current_ids;
            for (const auto& point : current_set) {
                current_ids.insert(point.id);
            }
            
            // Intersection: keep only IDs present in both sets
            std::set<long> new_result_ids;
            for (long id : result_ids) {
                if (current_ids.count(id)) {
                    new_result_ids.insert(id);
                }
            }
            result_ids = new_result_ids;
        }
        
        // Convert back to points
        return getPointsByIds(result_ids);
    }
    
    std::vector<InspectionPoint> executeOrOperation(const SimpleJsonParser::OrOperation& op) {
        std::set<long> result_ids;
        
        // Union of all operands
        for (const auto& operand : op.operands) {
            auto current_set = executeOperation(operand);
            for (const auto& point : current_set) {
                result_ids.insert(point.id);
            }
        }
        
        // Convert back to points
        return getPointsByIds(result_ids);
    }
    
    std::vector<InspectionPoint> getPointsByIds(const std::set<long>& ids) {
        if (ids.empty()) {
            return {};
        }
        
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        std::string query = "SELECT id, group_id, coord_x, coord_y, category FROM inspection_region WHERE id IN (";
        bool first = true;
        for (long id : ids) {
            if (!first) query += ", ";
            query += std::to_string(id);
            first = false;
        }
        query += ")";
        
        auto result = txn.exec(query);
        std::vector<InspectionPoint> points;
        
        for (size_t i = 0; i < result.size(); ++i) {
            InspectionPoint point;
            point.id = result[i]["id"].as<long>();
            point.group_id = result[i]["group_id"].as<long>();
            point.x = result[i]["coord_x"].as<double>();
            point.y = result[i]["coord_y"].as<double>();
            point.category = result[i]["category"].as<int>();
            points.push_back(point);
        }
        
        return points;
    }
    
    std::string buildCropQuery(const SimpleJsonParser::CropParams& params) {
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
                "        MIN(coord_x) >= " + std::to_string(params.region.p_min_x) + 
                " AND MAX(coord_x) <= " + std::to_string(params.region.p_max_x) + 
                " AND MIN(coord_y) >= " + std::to_string(params.region.p_min_y) + 
                " AND MAX(coord_y) <= " + std::to_string(params.region.p_max_y) +
                ") proper_groups ON ir.group_id = proper_groups.group_id ";
        }
        
        query += "WHERE "
                 "ir.coord_x >= " + std::to_string(params.region.p_min_x) + 
                 " AND ir.coord_x <= " + std::to_string(params.region.p_max_x) + 
                 " AND ir.coord_y >= " + std::to_string(params.region.p_min_y) + 
                 " AND ir.coord_y <= " + std::to_string(params.region.p_max_y);
        
        // Add category filter if specified
        if (params.has_category) {
            query += " AND ir.category = " + std::to_string(params.category);
        }
        
        // Add groups filter if specified
        if (params.has_one_of_groups && !params.one_of_groups.empty()) {
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
        std::ofstream file(output_file);
        if (!file.is_open()) {
            std::cerr << "Cannot open output file: " << output_file << std::endl;
            return false;
        }
        
        // Write points in format: x y
        for (size_t i = 0; i < points.size(); ++i) {
            file << points[i].x << " " << points[i].y << std::endl;
        }
        
        std::cout << "Output written to: " << output_file << " with " << points.size() << " points" << std::endl;
        return true;
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
    
    // Database connection
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