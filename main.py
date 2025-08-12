import os
import json
import xml.etree.ElementTree as ET
import re
from xml.dom import minidom

def compare_ddl_and_sxml_columns(ddl_string, sxml_string):
    """
    Compares column definitions between the DDL (CREATE TABLE statement)
    and the SXML metadata.

    Args:
        ddl_string (str): The SQL DDL content from the file.
        sxml_string (str): The SXML content from the snapshot.

    Returns:
        tuple: A tuple containing (messages, columns_in_ddl_only, columns_in_sxml_only).
    """
    messages = []
    
    # 1. Extract columns from the DDL using regex
    ddl_columns = set()
    create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
    if create_table_match:
        columns_block = create_table_match.group(1)
        found_columns = re.findall(r'^\s*"([^"]+)"', columns_block, re.MULTILINE)
        ddl_columns = set(c.upper() for c in found_columns)

    # 2. Extract columns from the SXML using an XML parser
    sxml_columns = set()
    try:
        root = ET.fromstring(sxml_string)
        # Define the namespace from the SXML, as it uses a default namespace
        ns = {'ku': 'http://xmlns.oracle.com/ku'}
        # Use the namespace in the XPath query to correctly find the elements
        for item in root.findall('.//ku:COL_LIST/ku:COL_LIST_ITEM/ku:NAME', ns):
            if item.text:
                sxml_columns.add(item.text.strip().upper())
    except ET.ParseError:
        messages.append("  COMPARISON FAILED: Could not parse SXML to extract columns.")
        return messages, set(), set()

    # 3. Compare the two sets of columns
    if not ddl_columns:
        messages.append("  COMPARISON WARNING: Could not find any columns in the DDL CREATE TABLE statement.")
    
    in_ddl_not_in_sxml = ddl_columns - sxml_columns
    in_sxml_not_in_ddl = sxml_columns - ddl_columns

    if in_ddl_not_in_sxml:
        messages.append(f"  Discrepancy: Columns in DDL but not in SXML -> {sorted(list(in_ddl_not_in_sxml))}")
    
    if in_sxml_not_in_ddl:
        messages.append(f"  Discrepancy: Columns in SXML but not in DDL -> {sorted(list(in_sxml_not_in_ddl))}")
        
    if not in_ddl_not_in_sxml and not in_sxml_not_in_ddl and ddl_columns:
        if not any("Discrepancy" in m or "WARNING" in m for m in messages):
             messages.append(f"  Column Check: OK. All {len(ddl_columns)} columns match between DDL and SXML.")

    return messages, in_ddl_not_in_sxml, in_sxml_not_in_ddl


def get_start_with_value(schema, table_name):
    """
    Generates the START_WITH value for an identity column.
    For now, it returns a static value.

    Args:
        schema (str): The schema name from the SXML.
        table_name (str): The table name from the SXML.

    Returns:
        int: The value for the START_WITH tag.
    """
    # This function can be expanded later if needed.
    return 1

def parse_sql_snapshot_files(root_folder):
    """
    Traverses a directory tree, finds *.sql files, and processes them
    to find and parse a '-- sqlcl_snapshot' line. Only reports on files
    with issues.

    Args:
        root_folder (str): The absolute or relative path to the folder
                           to start searching from.
    """
    # Check if the provided root_folder exists and is a directory
    if not os.path.isdir(root_folder):
        print(f"Error: The specified folder '{root_folder}' does not exist or is not a directory.")
        return

    print(f"Starting scan in folder: '{root_folder}'.")
    print("IMPORTANT: This script will modify files in place if corrections are made.\n")

    # os.walk() efficiently traverses the directory tree (top-down)
    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            # Check if the file has a .sql extension
            if filename.endswith(".sql"):
                # Construct the full path to the file
                file_path = os.path.join(dirpath, filename)
                process_single_file(file_path)


def process_single_file(file_path):
    """
    Reads a single SQL file, looks for the snapshot line, parses it,
    and if a fix is applied, it overwrites the original file.

    Args:
        file_path (str): The full path to the SQL file to process.
    """
    snapshot_prefix = "-- sqlcl_snapshot"
    messages = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        file_was_modified = False
        
        for i, line in enumerate(lines):
            if line.strip().startswith(snapshot_prefix):
                original_line_index = i
                json_string = line.strip()[len(snapshot_prefix):].strip()
                
                if json_string:
                    try:
                        data = json.loads(json_string)
                        sxml_string = data.get("sxml")

                        if sxml_string:
                            sxml_to_check = sxml_string
                            corrected_sxml = None
                            
                            try:
                                ET.fromstring(sxml_string)
                            except ET.ParseError as xml_err:
                                # SXML parsing failed, attempt to fix it.
                                open_count = sxml_string.count('<IDENTITY_COLUMN>')
                                close_count = sxml_string.count('</IDENTITY_COLUMN>')

                                # Attempt to fix missing </IDENTITY_COLUMN> and add new tags
                                if open_count > close_count:
                                    schema_match = re.search(r'<SCHEMA>(.*?)</SCHEMA>', sxml_string)
                                    name_match = re.search(r'<NAME>(.*?)</NAME>', sxml_string)
                                    if schema_match and name_match:
                                        schema = schema_match.group(1)
                                        table_name = name_match.group(1)
                                        start_with_val = get_start_with_value(schema, table_name)
                                        tags_to_add = f"""<GENERATION>DEFAULT</GENERATION><ON_NULL></ON_NULL><START_WITH>{start_with_val}</START_WITH><INCREMENT>1</INCREMENT><MINVALUE>1</MINVALUE><MAXVALUE>9999999999999999999999999999</MAXVALUE><CACHE>20</CACHE></IDENTITY_COLUMN>"""
                                        
                                        start_pos = sxml_string.find('<IDENTITY_COLUMN>')
                                        schema_end_tag = '</SCHEMA>'
                                        schema_end_pos = sxml_string.find(schema_end_tag, start_pos)
                                        
                                        if schema_end_pos != -1:
                                            insertion_point = schema_end_pos + len(schema_end_tag)
                                            temp_sxml = sxml_string[:insertion_point] + tags_to_add + sxml_string[insertion_point:]
                                            try:
                                                ET.fromstring(temp_sxml)
                                                corrected_sxml = temp_sxml
                                                messages.append(f"SUCCESS (Line {i+1}): File updated. Added missing tags and closing </IDENTITY_COLUMN>.")
                                            except ET.ParseError: pass
                                
                                if corrected_sxml:
                                    # Update the sxml value, but keep the original hash
                                    data['sxml'] = corrected_sxml
                                    sxml_to_check = corrected_sxml # Use the corrected version for the check
                                    
                                    updated_json_string = json.dumps(data, separators=(',', ':'))
                                    lines[original_line_index] = f"-- sqlcl_snapshot {updated_json_string}\n"
                                    file_was_modified = True
                                else:
                                    messages.append(f"ERROR (Line {i+1}): Unfixable SXML parse error. Reason: {xml_err}")
                                    sxml_to_check = None # Can't check an unfixable SXML

                            # Perform the DDL vs SXML comparison on the final version of the SXML
                            if sxml_to_check:
                                ddl_content = "".join(lines[:original_line_index])
                                comparison_messages, in_ddl_not_in_sxml, in_sxml_not_in_ddl = compare_ddl_and_sxml_columns(ddl_content, sxml_to_check)
                                
                                has_discrepancy = bool(in_ddl_not_in_sxml or in_sxml_not_in_ddl)

                                # If there's a problem, report it and create a log file.
                                if file_was_modified or has_discrepancy:
                                    messages.extend(comparison_messages)
                                    
                                    # If there was a discrepancy, create the log file with formatted SXML
                                    if has_discrepancy:
                                        try:
                                            # Create the log file path
                                            log_file_path = os.path.splitext(file_path)[0] + ".log"
                                            
                                            dom = minidom.parseString(sxml_to_check)
                                            
                                            # Mark columns that are in SXML but not DDL
                                            col_list_items = dom.getElementsByTagName('COL_LIST_ITEM')
                                            for item in col_list_items:
                                                name_nodes = item.getElementsByTagName('NAME')
                                                if name_nodes and name_nodes[0].firstChild:
                                                    col_name = name_nodes[0].firstChild.nodeValue.upper()
                                                    if col_name in in_sxml_not_in_ddl:
                                                        comment = dom.createComment(f" COLUMN '{col_name}' IS MISSING FROM DDL ")
                                                        item.parentNode.insertBefore(comment, item)

                                            ugly_xml = dom.toprettyxml(indent="  ")
                                            good_lines = [line for line in ugly_xml.split('\n') if line.strip()]
                                            formatted_sxml = "\n".join(good_lines)
                                            
                                            with open(log_file_path, 'w', encoding='utf-8') as log_f:
                                                # Write header comment with summary
                                                log_f.write("<!--\n  Column Discrepancy Report\n\n")
                                                if in_ddl_not_in_sxml:
                                                    log_f.write(f"  - Columns in DDL but not SXML: {sorted(list(in_ddl_not_in_sxml))}\n")
                                                if in_sxml_not_in_ddl:
                                                    log_f.write(f"  - Columns in SXML but not DDL: {sorted(list(in_sxml_not_in_ddl))}\n")
                                                log_f.write("-->\n\n")

                                                # Write original DDL
                                                log_f.write("<!-- Original DDL from .sql file -->\n")
                                                log_f.write(ddl_content.strip() + "\n\n")
                                                
                                                # Write SXML
                                                log_f.write("<!-- SXML Metadata from snapshot (with annotations) -->\n")
                                                log_f.write(formatted_sxml)

                                            messages.append(f"  INFO: Discrepancy details saved to: {log_file_path}")
                                        except Exception as e:
                                            messages.append(f"  ERROR: Could not write log file. Reason: {e}")

                        else:
                            messages.append(f"WARNING (Line {i+1}): JSON data is missing the 'sxml' key.")
                    except json.JSONDecodeError as json_err:
                        messages.append(f"ERROR (Line {i+1}): Failed to parse JSON. Reason: {json_err}")
                else:
                    messages.append(f"WARNING (Line {i+1}): Snapshot line is empty and contains no JSON data.")
                
                break # Only process first snapshot line
        
        # If any corrections were made, write the changes back to the file.
        if file_was_modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)

    except IOError as e:
        messages.append(f"ERROR: Could not read file. Reason: {e}")
    except Exception as e:
        messages.append(f"ERROR: An unexpected error occurred: {e}")

    # Only print anything if there are messages to report
    if messages:
        print(f"--- Issues found in file: {file_path} ---")
        for msg in messages:
            print(f"  {msg}")
        print("-" * (len(file_path) + 25) + "\n")


if __name__ == "__main__":
    # --- IMPORTANT ---
    # Change this path to the folder you want to scan.
    # You can use a relative path (like './my_folder') or an
    # absolute path (like 'C:/Users/YourUser/Documents/sql_scripts').
    target_directory = "./src/database/xxxx/tables" 
    
    # Create a dummy folder and files for demonstration if the target doesn't exist
    if not os.path.exists(target_directory):
        print(f"'{target_directory}' not found. Creating a demo setup...")
        os.makedirs(os.path.join(target_directory, "subfolder"))
        
        # File 1: Valid case
        with open(os.path.join(target_directory, "good_file.sql"), "w") as f:
            f.write("SELECT * FROM employees;\n")
            f.write('-- sqlcl_snapshot {"hash":"abcde12345","type":"TABLE","name":"EMPLOYEES","schemaName":"HR","sxml":"<TABLE DDL_VERSION=\\"2\\"><COL_LIST><COL_LIST_ITEM>...</COL_LIST_ITEM></COL_LIST>"}\n')
            f.write("SELECT * FROM departments;\n")

        # File 2: Invalid JSON
        with open(os.path.join(target_directory, "bad_json.sql"), "w") as f:
            f.write('-- sqlcl_snapshot {"hash":"fghij67890", "sxml": "<root/>",,}\n')

        # File 3: Invalid SXML
        with open(os.path.join(target_directory, "subfolder", "bad_sxml.sql"), "w") as f:
            f.write('-- sqlcl_snapshot {"hash":"klmno11223","sxml":"<root><unclosed-tag></root>"}\n')
            
        # File 4: No snapshot line
        with open(os.path.join(target_directory, "subfolder", "no_snapshot.sql"), "w") as f:
            f.write("CREATE VIEW my_view AS SELECT 1 FROM DUAL;\n")
            
    # Run the main function
    parse_sql_snapshot_files(target_directory)
