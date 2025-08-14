#!/usr/bin/env python3
import argparse
import json
import sys

def find_closing_bracket(data):
    """Finds the index of the closing bracket for a given opening bracket."""
    depth = 1
    for i, char in enumerate(data):
        if char == '[':
            depth += 1
        elif char == ']':
            depth -= 1
            if depth == 0:
                return i + 1
    raise ValueError(f"No matching closing bracket found '{data}'")

def parse(data):
    """Parses the input data and converts it into a Python object."""
    data = data.strip()
    if data.startswith("Str(\""):
        end = 5 + data[5:].find("\")")
        val = data[5:end]
        rest = data[end+2:]
        return val, rest
    elif data.startswith("Float("):
        end = 6 + data[6:].find(")")
        rest = data[end+1:]
        val = data[6:end]
        return float(val), rest
    elif data.startswith("Int("):
        end = 4 + data[4:].find(")")
        rest = data[end+1:]
        val = data[4:end]
        return int(val), rest
    elif data.startswith("Bool("):
        end = 5 + data[5:].find(")")
        rest = data[end+1:]
        val = data[5:-1]
        return val.lower() == "true", rest
    elif data.startswith("List(["):
        end = 6 + find_closing_bracket(data[6:])
        inner = []
        rem = data[6:end-1]
        while rem != "":
            if rem.startswith(","):
                rem = rem[1:].strip()
            inner_raw = parse(rem)
            inner.append(inner_raw[0])
            rem = inner_raw[1]
        rest = data[end+1:]
        return inner, rest
    else:
        raise ValueError(f"Unknown atom format: {data}")

def special_json_rule(data):
    """Transforms the parsed data according to a specific key-value rule."""
    # Recursively traverse the data, every element that is in format: [str, val] must be turned into {str: val}
    if isinstance(data, list):
        if len(data) == 2 and isinstance(data[0], str):
            return {data[0]: special_json_rule(data[1])}
        else:
            return [special_json_rule(item) for item in data]
    else:
        return data

def consume_assignment(data):
    assignment = data.find("=")
    if assignment == -1:
        return "", data.strip()
    else:
        return data[:assignment].strip(), data[assignment + 1:].strip()

def transform(data):
    data = data.strip()
    assignment, data = consume_assignment(data)
    data, rest = parse(data)
    if rest.strip():
        raise ValueError(f"Unexpected trailing data after parsing: '{rest}'")
    # print(f"Parsed data: {data}")
    data = special_json_rule(data)
    # print(f"Transformed data according to special rule: {data}")
    return assignment, data



# --- Transform according to the key-value rule ---

def main():
    parser = argparse.ArgumentParser(
        description="Convert pseudo List/Str/Float/Int/Bool syntax into JSON."
    )
    parser.add_argument(
        "input_file", help="Path to the input file containing the data."
    )
    parser.add_argument("output_file", help="Path to the output JSON file.")
    args = parser.parse_args()

    try:
        with open(args.input_file, "r", encoding="utf-8") as f:
            data = f.readlines()
    except FileNotFoundError:
        sys.exit(f"Error: Input file '{args.input_file}' not found.")

    result = map(transform, data)
    result = list(result)

    with open(args.output_file, "w", encoding="utf-8") as f:
        # Write: assignment = value
        for assignment, value in result:
            if assignment:
                f.write(f"{assignment} = {json.dumps(value, indent=2)}\n")
            else:
                f.write(f"{json.dumps(value, indent=2)}\n")

    print(f"✅ Converted '{args.input_file}' → '{args.output_file}'")


if __name__ == "__main__":
    main()
