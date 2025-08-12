import os
import json
import xml.etree.ElementTree as ET
import re
from xml.dom import minidom

def add_missing_columns_to_sxml(missing_columns, ddl_string, sxml_string):
    """
    Parses the DDL for missing columns, generates their SXML representation,
    and adds them to the main SXML string.

    Args:
        missing_columns (set): A set of uppercase column names missing from the SXML.
        ddl_string (str): The full DDL content.
        sxml_string (str): The original SXML string.

    Returns:
        str: The updated SXML string with the new columns added.
    """
    new_col_items = []
    # Find the full CREATE TABLE block to parse from
    create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
    if not create_table_match:
        return sxml_string # Cannot proceed without a DDL block

    columns_block = create_table_match.group(1)
    
    for col_name in missing_columns:
        # Find the full definition line for the missing column
        # This regex looks for a line starting with the quoted column name
        col_def_match = re.search(r'^\s*"' + re.escape(col_name) + r'"\s+(.*)', columns_block, re.MULTILINE | re.IGNORECASE)
        if not col_def_match:
            continue

        col_def = col_def_match.group(1).strip().rstrip(',')
        
        # Build the SXML fragment for this column
        item_xml = f'      <COL_LIST_ITEM>\n        <NAME>{col_name}</NAME>\n'
        
        # Handle different data types
        type_def_upper = col_def.upper()
        if type_def_upper.startswith('VARCHAR2'):
            item_xml += '        <DATATYPE>VARCHAR2</DATATYPE>\n'
            length_match = re.search(r'\((\d+)', col_def)
            if length_match:
                item_xml += f'        <LENGTH>{length_match.group(1)}</LENGTH>\n'
            item_xml += '        <CHAR_SEMANTICS></CHAR_SEMANTICS>\n'
            item_xml += '        <COLLATE_NAME>USING_NLS_COMP</COLLATE_NAME>\n'
        elif type_def_upper.startswith('NUMBER'):
            item_xml += '        <DATATYPE>NUMBER</DATATYPE>\n'
            precision_match = re.search(r'NUMBER\((\d+),(\d+)\)', type_def_upper)
            if precision_match:
                item_xml += f'        <PRECISION>{precision_match.group(1)}</PRECISION>\n'
                item_xml += f'        <SCALE>{precision_match.group(2)}</SCALE>\n'
        elif type_def_upper.startswith('DATE'):
            item_xml += '        <DATATYPE>DATE</DATATYPE>\n'
        elif type_def_upper.startswith('CLOB'):
            item_xml += '        <DATATYPE>CLOB</DATATYPE>\n'
            item_xml += '        <COLLATE_NAME>USING_NLS_COMP</COLLATE_NAME>\n'
        elif type_def_upper.startswith('BLOB'):
            item_xml += '        <DATATYPE>BLOB</DATATYPE>\n'

        if 'NOT NULL' in type_def_upper:
            item_xml += '        <NOT_NULL></NOT_NULL>\n'
            
        item_xml += '      </COL_LIST_ITEM>\n'
        new_col_items.append(item_xml)

    # Insert the new column items into the main SXML string before the closing </COL_LIST>
    if new_col_items:
        col_list_end_tag = '</COL_LIST>'
        # Use find() to get the FIRST occurrence, which is the main column list
        insertion_point = sxml_string.find(col_list_end_tag)
        if insertion_point != -1:
            updated_sxml = sxml_string[:insertion_point] + "".join(new_col_items) + sxml_string[insertion_point:]
            return updated_sxml

    return sxml_string


def compare_ddl_and_sxml_columns(ddl_string, sxml_string):
    """
    Performs a deep comparison of columns between DDL and SXML, checking for
    existence, data types, and attributes.

    Args:
        ddl_string (str): The SQL DDL content from the file.
        sxml_string (str): The SXML content from the snapshot.

    Returns:
        tuple: A tuple containing (messages, in_ddl_not_in_sxml, in_sxml_not_in_ddl, attribute_mismatches).
    """
    messages = []
    ddl_cols = {}
    sxml_cols = {}
    
    # 1. Parse DDL to extract detailed column attributes
    create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
    if create_table_match:
        columns_block = create_table_match.group(1)
        # Regex to capture column name and its full definition on the same line
        col_definitions = re.findall(r'^\s*"([^"]+)"\s+(.*)', columns_block, re.MULTILINE | re.IGNORECASE)
        for name, definition in col_definitions:
            name = name.upper()
            definition = definition.strip().rstrip(',')
            ddl_cols[name] = {'not_null': 'NOT NULL' in definition.upper()}
            
            type_def = definition.upper().split()[0]
            if type_def.startswith('VARCHAR2'):
                ddl_cols[name]['type'] = 'VARCHAR2'
                length_match = re.search(r'\((\d+)', definition)
                ddl_cols[name]['length'] = length_match.group(1) if length_match else None
            elif type_def.startswith('NUMBER'):
                ddl_cols[name]['type'] = 'NUMBER'
                # Use re.search on the whole definition to be more robust
                match = re.search(r'NUMBER\((\d+),(\d+)\)', definition, re.IGNORECASE)
                if match:
                    ddl_cols[name]['precision'] = match.group(1)
                    ddl_cols[name]['scale'] = match.group(2)
                else:
                    match = re.search(r'NUMBER\((\d+)\)', definition, re.IGNORECASE)
                    if match:
                        ddl_cols[name]['precision'] = match.group(1)
                        ddl_cols[name]['scale'] = '0' # Oracle default
            elif type_def.startswith('DATE'):
                ddl_cols[name]['type'] = 'DATE'
            elif type_def.startswith('CLOB'):
                ddl_cols[name]['type'] = 'CLOB'
            elif type_def.startswith('BLOB'):
                ddl_cols[name]['type'] = 'BLOB'

    # 2. Parse SXML to extract detailed column attributes
    try:
        root = ET.fromstring(sxml_string)
        ns = {'ku': 'http://xmlns.oracle.com/ku'}
        main_col_list = root.find('.//ku:RELATIONAL_TABLE/ku:COL_LIST', ns)
        if main_col_list is not None:
            for item in main_col_list.findall('./ku:COL_LIST_ITEM', ns):
                name_node = item.find('ku:NAME', ns)
                if name_node is not None and name_node.text:
                    name = name_node.text.strip().upper()
                    sxml_cols[name] = {
                        'type': item.findtext('ku:DATATYPE', '', ns),
                        'length': item.findtext('ku:LENGTH', None, ns),
                        'precision': item.findtext('ku:PRECISION', None, ns),
                        'scale': item.findtext('ku:SCALE', None, ns),
                        'not_null': item.find('ku:NOT_NULL', ns) is not None
                    }
    except ET.ParseError:
        messages.append("  COMPARISON FAILED: Could not parse SXML to extract columns.")
        return messages, set(), set(), []

    # 3. Compare the two dictionaries
    ddl_col_names = set(ddl_cols.keys())
    sxml_col_names = set(sxml_cols.keys())
    
    in_ddl_not_in_sxml = ddl_col_names - sxml_col_names
    in_sxml_not_in_ddl = sxml_col_names - ddl_col_names
    attribute_mismatches = []

    if in_ddl_not_in_sxml:
        messages.append(f"  Discrepancy: Columns in DDL but not in SXML -> {sorted(list(in_ddl_not_in_sxml))}")
    
    if in_sxml_not_in_ddl:
        messages.append(f"  Discrepancy: Columns in SXML but not in DDL -> {sorted(list(in_sxml_not_in_ddl))}")

    # 4. Check for attribute mismatches on common columns
    common_cols = ddl_col_names.intersection(sxml_col_names)
    for col in common_cols:
        ddl_attr = ddl_cols[col]
        sxml_attr = sxml_cols[col]
        mismatches = []
        
        # Compare attributes
        if ddl_attr.get('type') != sxml_attr.get('type'):
            mismatches.append(f"Type mismatch: DDL='{ddl_attr.get('type')}', SXML='{sxml_attr.get('type')}'")
        if ddl_attr.get('length') != sxml_attr.get('length'):
            mismatches.append(f"Length mismatch: DDL='{ddl_attr.get('length')}', SXML='{sxml_attr.get('length')}'")
        if ddl_attr.get('precision') != sxml_attr.get('precision'):
            mismatches.append(f"Precision mismatch: DDL='{ddl_attr.get('precision')}', SXML='{sxml_attr.get('precision')}'")
        if ddl_attr.get('scale') != sxml_attr.get('scale'):
            mismatches.append(f"Scale mismatch: DDL='{ddl_attr.get('scale')}', SXML='{sxml_attr.get('scale')}'")
        if ddl_attr.get('not_null') != sxml_attr.get('not_null'):
            mismatches.append(f"NOT NULL mismatch: DDL='{ddl_attr.get('not_null')}', SXML='{sxml_attr.get('not_null')}'")
            
        if mismatches:
            messages.append(f"  Attribute Mismatch on column '{col}': {'; '.join(mismatches)}")
            attribute_mismatches.append({'column': col, 'details': mismatches})

    return messages, in_ddl_not_in_sxml, in_sxml_not_in_ddl, attribute_mismatches


def get_start_with_value(schema, table_name):
    return 1

def fix_identity_column(sxml_string):
    """
    Checks for and fixes a missing IDENTITY_COLUMN closing tag.
    
    Returns:
        tuple: (corrected_sxml_string, message)
    """
    open_count = sxml_string.count('<IDENTITY_COLUMN>')
    close_count = sxml_string.count('</IDENTITY_COLUMN>')

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
                corrected_sxml = sxml_string[:insertion_point] + tags_to_add + sxml_string[insertion_point:]
                try:
                    ET.fromstring(corrected_sxml)
                    message = "File updated. Added missing </IDENTITY_COLUMN> tag."
                    return corrected_sxml, message
                except ET.ParseError:
                    return None, "SXML still invalid after IDENTITY_COLUMN fix."
    return None, None


def generate_log_file(file_path, ddl_content, original_sxml, corrected_sxml, discrepancies):
    """
    Creates a detailed .log file for a given SQL file with discrepancies.
    """
    in_ddl, in_sxml, mismatches = discrepancies
    try:
        log_file_path = os.path.splitext(file_path)[0] + ".log"
        with open(log_file_path, 'w', encoding='utf-8') as log_f:
            log_f.write("<!--\n  Discrepancy Report\n\n")
            if in_ddl:
                log_f.write(f"  - Columns in DDL but not SXML: {sorted(list(in_ddl))}\n")
            if in_sxml:
                log_f.write(f"  - Columns in SXML but not DDL: {sorted(list(in_sxml))}\n")
            if mismatches:
                log_f.write("  - Attribute Mismatches:\n")
                for m in mismatches:
                    log_f.write(f"    - Column '{m['column']}': {'; '.join(m['details'])}\n")
            log_f.write("-->\n\n")
            
            log_f.write("<!-- Original DDL from .sql file -->\n")
            log_f.write(ddl_content.strip() + "\n\n")
            
            dom_original = minidom.parseString(original_sxml)
            ugly_xml_original = dom_original.toprettyxml(indent="  ")
            good_lines_original = [line for line in ugly_xml_original.split('\n') if line.strip()]
            formatted_sxml_original = "\n".join(good_lines_original)
            log_f.write("<!-- Original SXML Metadata from snapshot -->\n")
            log_f.write(formatted_sxml_original + "\n\n")

            dom_corrected = minidom.parseString(corrected_sxml)
            ugly_xml_corrected = dom_corrected.toprettyxml(indent="  ")
            good_lines_corrected = [line for line in ugly_xml_corrected.split('\n') if line.strip()]
            formatted_sxml_corrected = "\n".join(good_lines_corrected)
            log_f.write("<!-- Final SXML Metadata (with fixes applied) -->\n")
            log_f.write(formatted_sxml_corrected)
        return f"INFO: Discrepancy details saved to: {log_file_path}"
    except Exception as e:
        return f"ERROR: Could not write log file. Reason: {e}"


def process_single_file(file_path):
    """
    Reads a single SQL file, looks for the snapshot line, parses it,
    and if a fix is applied, it overwrites the original file.
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
                
                if not json_string:
                    messages.append(f"WARNING (Line {i+1}): Snapshot line is empty.")
                    break
                
                try:
                    data = json.loads(json_string)
                    original_sxml = data.get("sxml")

                    if not original_sxml:
                        messages.append(f"WARNING (Line {i+1}): JSON data is missing the 'sxml' key.")
                        break

                    sxml_to_process = original_sxml
                    
                    # Step 1: Check if SXML is valid. If not, try to fix it.
                    try:
                        ET.fromstring(sxml_to_process)
                    except ET.ParseError as xml_err:
                        corrected_sxml, fix_message = fix_identity_column(sxml_to_process)
                        if corrected_sxml:
                            sxml_to_process = corrected_sxml
                            messages.append(f"SUCCESS (Line {i+1}): {fix_message}")
                            file_was_modified = True
                        else:
                            messages.append(f"ERROR (Line {i+1}): {fix_message or f'Unfixable SXML parse error: {xml_err}'}")
                            sxml_to_process = None # Stop processing this file

                    # Step 2: If we have valid SXML, perform content validation and correction
                    if sxml_to_process:
                        ddl_content = "".join(lines[:original_line_index])
                        comp_messages, in_ddl, in_sxml, mismatches = compare_ddl_and_sxml_columns(ddl_content, sxml_to_process)
                        
                        # Step 3: Add any columns that are missing from the SXML
                        if in_ddl:
                            sxml_to_process = add_missing_columns_to_sxml(in_ddl, ddl_content, sxml_to_process)
                            messages.append(f"SUCCESS (Line {i+1}): Added missing columns to SXML: {sorted(list(in_ddl))}")
                            file_was_modified = True

                        # Step 4: Check for discrepancies to generate logs
                        has_discrepancy = bool(in_ddl or in_sxml or mismatches)
                        if has_discrepancy:
                            messages.extend(comp_messages)
                            discrepancies = (in_ddl, in_sxml, mismatches)
                            log_message = generate_log_file(file_path, ddl_content, original_sxml, sxml_to_process, discrepancies)
                            messages.append(f"  {log_message}")
                    
                    # Step 5: If any changes were made, update the file content
                    if file_was_modified:
                        data['sxml'] = sxml_to_process
                        lines[original_line_index] = f"-- sqlcl_snapshot {json.dumps(data, separators=(',', ':'))}\n"

                except json.JSONDecodeError as json_err:
                    messages.append(f"ERROR (Line {i+1}): Failed to parse JSON. Reason: {json_err}")
                
                break # Only process first snapshot line
        
        if file_was_modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)

    except IOError as e:
        messages.append(f"ERROR: Could not read file. Reason: {e}")
    except Exception as e:
        messages.append(f"ERROR: An unexpected error occurred: {e}")

    if messages:
        print(f"--- Issues found in file: {file_path} ---")
        for msg in messages:
            print(f"  {msg}")
        print("-" * (len(file_path) + 25) + "\n")


def parse_sql_snapshot_files(root_folder):
    if not os.path.isdir(root_folder):
        print(f"Error: The specified folder '{root_folder}' does not exist or is not a directory.")
        return

    print("Cleaning up old .log files...")
    log_files_removed = 0
    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(".log"):
                log_path = os.path.join(dirpath, filename)
                try:
                    os.remove(log_path)
                    log_files_removed += 1
                except OSError as e:
                    print(f"  Warning: Could not remove log file '{log_path}'. Reason: {e}")
    print(f"Removed {log_files_removed} old log file(s).")

    print(f"\nStarting scan in folder: '{root_folder}'.")
    print("IMPORTANT: This script will modify files in place if corrections are made.\n")

    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(".sql"):
                file_path = os.path.join(dirpath, filename)
                process_single_file(file_path)




if __name__ == "__main__":
    # --- IMPORTANT ---
    # Change this path to the folder you want to scan.
    # You can use a relative path (like './my_folder') or an
    # absolute path (like 'C:/Users/YourUser/Documents/sql_scripts').
    target_directory = "" 
    
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
