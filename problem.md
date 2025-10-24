# Context
We manage inspection regions as 2D points. Each region has a coordinate, a
category, and belongs to a group (partition). Your goal is to load the data
into PostgreSQL and implement a query module in C++.

# Task 1: DBMS setup and Data Loading
Implement a C++ console program that:
1. Takes an argument --data_directory (path to folder containing the files).
2. Reads the three text files.
3. Populates the PostgreSQL database using libpqxx.

You will set up the postgresql DBMS in your operating system. We highly
recommend installing pgAdmin4 along with the DBMS. It will assist your
problem solving and debugging. Before continue reading this problem
description, Please skim the schema.pgsql file for the entity attributes
and the relationship.

The data is provided in a directory containing:
* points.txt — Each line: x y
* categories.txt — Each line: category ID for the corresponding point
* groups.txt — Each line: group ID for the corresponding point
(Line i in all three files corresponds to the same region.)

You may use a command line parser (gflags/boost::program_options) for the tasks.

# Task 2: Querying Regions.
Write another C++ console program to query points based on a JSON input file
(--query=q1.json). This program should output a text file containing the
selected points, sorted by (y, x)

## Definitions
* Valid region: Points lying inside the axis-aligned rectangle [p_min, p_max].
* Proper point: A point whose entire group lies within the valid region.

## Supported Operator: operator_crop
 Selects points that meet the following:
* region (required) — rectangular bounds.
* category (optional) — only include points of this category.
* one_of_groups (optional) — only include points whose group_id is in the list.
* proper (optional) — if true, only include points whose entire group is valid.

```JSON
{
  "valid_region": {
    "p_min": { "x": 0, "y": 0 },
    "p_max": { "x": 400, "y": 300 }
  },
  "query": {
    "operator_crop": {
      "region": {
        "p_min": { "x": 200, "y": 200 },
        "p_max": { "x": 400, "y": 300 }
      },
      "category": 1,
      "one_of_groups": [0, 5],
      "proper": true
    }
  }
}
```

# Task 3 (optional): Extended Operators
Extend your program to support logical operators:
* operator_and — intersection of results
* operator_or — union of results

Operators can be nested arbitrarily. The "and" operator computes the
intersection of all set operands whereas the "or" operator computes the union.

Example Input
```JSON
{
  "valid_region": {
    "p_min": { "x": 0, "y": 0 },
    "p_max": { "x": 1000, "y": 1000 }
  },
  "query": {
    "operator_and": [
      {
        "operator_crop": {
          "region": {
            "p_min": { "x": 200, "y": 200 },
            "p_max": { "x": 400, "y": 300 }
          }
        }
      },
      {
        "operator_or": [
          {
            "operator_crop": {
              "region": {
                "p_min": { "x": 100, "y": 100 },
                "p_max": { "x": 250, "y": 1000 }
              }
            }
          },
          {
            "operator_crop": {
              "region": {
                "p_min": { "x": 350, "y": 100 },
                "p_max": { "x": 500, "y": 1000 }
              },
              "proper": true
            }
          }
        ]
      }
    ]
  }
}
```
If you are out of time, feel free to lay down your extension plan in solution 2.

# Implementation Requirements
* Must compile via CMake, Ninja, Makefile, or Visual Studio.
* Use libpqxx for PostgreSQL access.
* Use a JSON library (e.g., nlohmann/rapidjson).
* Focus on robustness, readability, and error handling.

| Aspect          | Description                            |
| --------------- | -------------------------------------- |
| Correctness     | Outputs match query semantics          |
| Code quality    | Clear, modular, uses good abstractions |
| Database design | Schema supports queries efficiently    |
| JSON handling   | Supports optional fields and nesting   |

Note, completing tasks with LLM tools is welcomed, but be sure that you
fully understand the solution.
