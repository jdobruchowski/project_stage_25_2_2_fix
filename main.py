import os
import json
import xml.etree.ElementTree as ET
import re
from xml.dom import minidom
import subprocess

# --- You will need these two helper functions ---
def are_sxml_semantically_equal(sxml_str1, sxml_str2):
    """
    Compares two SXML strings for semantic equality using a robust
    normalization method that strips all whitespace between tags.
    """
    try:
        # First, remove the XML declaration which can have inconsistent formatting
        sxml_str1 = re.sub(r'<\?xml.*?\?>', '', sxml_str1).strip()
        sxml_str2 = re.sub(r'<\?xml.*?\?>', '', sxml_str2).strip()

        # Use regex to replace any whitespace (\s+) ONLY between tags (><)
        normalized_str1 = re.sub(r'>\s+<', '><', sxml_str1)
        normalized_str2 = re.sub(r'>\s+<', '><', sxml_str2)

        # Now, compare the correctly normalized strings
        return normalized_str1 == normalized_str2
    except Exception:
        return False

# --- The Main Function with the Loop ---
def get_git_diff(file_path,repo):
    """
    Iteratively runs git diff and applies fixes until the diff is clean
    or contains an unfixable, meaningful change.
    """
    applied_fixes = []
    final_diff_output = ""
    # A safety break to prevent any theoretical infinite loops
    if file_path == '/Users/jdobruchowski/Documents/Git/Praca/BeachCourse/beachcourse/project/src/database/gen/tables/inventory_detail.log':
         t=1

    for _ in range(5): 
        try:
            repo_directory = os.path.dirname(file_path)
            command = ['git', '-C', repo_directory, 'diff', '--unified=0', repo, '--', file_path]
            result = subprocess.run(command, capture_output=True, text=True, check=False)

            if result.returncode != 0 and result.stderr:
                return f"Git command failed.\nError:\n{result.stderr}"
        except Exception as e:
            return f"An unexpected error occurred during git diff: {e}"

        current_diff = result.stdout
        final_diff_output = current_diff

        # --- Exit Condition 1: The diff is clean ---
        if not current_diff:
            break
        # --- Fix Condition 2: "No Newline" Warning ---
        if '\\ No newline at end of file' in current_diff:
                pos_warning = current_diff.find(r'\ No newline at end of file')
                pos_snapshot = current_diff.find('+-- sqlcl_snapshot')
                if pos_warning != -1 and pos_snapshot != -1:
                    if pos_warning < pos_snapshot:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(content.rstrip())
                    else:
                         with open(file_path, 'a', encoding='utf-8') as f:
                            f.write('\n')          
                    applied_fixes.append("Git Fix Applied: Corrected trailing newline issue.")
                    continue # Restart the loop to check again



        # --- Fix Condition 1: Cosmetic SXML Change ---
        changed_lines = [line for line in current_diff.splitlines() if line.startswith('+-- sqlcl_snapshot ') or line.startswith('--- sqlcl_snapshot ')]
        if len(changed_lines) == 2 and changed_lines[0].startswith('--- ') and changed_lines[1].startswith('+-- '):
            old_line = changed_lines[0][1:]
            new_line = changed_lines[1][1:]
            if old_line.strip().startswith('-- sqlcl_snapshot'):
                try:
                    original_sxml = json.loads(old_line.strip()[len('-- sqlcl_snapshot'):].strip()).get('sxml', '')
                    perfected_sxml = json.loads(new_line.strip()[len('-- sqlcl_snapshot'):].strip()).get('sxml', '')
                    if are_sxml_semantically_equal(original_sxml, perfected_sxml):
                        corrected_line = old_line
                        
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_content_lines = f.readlines()
                        for i, line in enumerate(file_content_lines):
                            if line.strip() == new_line.strip():
                                file_content_lines[i] = corrected_line + '\n'
                                break
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.writelines(file_content_lines)
                        
                        applied_fixes.append("Git Fix Applied: Synchronized SXML formatting with repo.")
                        continue # Restart the loop to check again
                except Exception:
                    raise
                    pass # If parsing fails, it's not a simple cosmetic fix


        # --- Exit Condition 2: Unfixable Diff ---
        # If we reach here, we found a diff but couldn't fix it. Exit the loop.
        break

    # --- Assemble the final report ---
    report = ""
    if applied_fixes:
        report += "\n".join(applied_fixes) + "\n\n"
    
    if not final_diff_output:
        report += "Git diff: Files are the same."
    else:
        report += "--- Final Diff ---\n" + final_diff_output

    return report

def reorder_sxml_columns_to_match_ddl(ddl_string, sxml_string):
    """
    Checks if the SXML column order matches the DDL order and corrects it if necessary.
    **MODIFIED to be more compatible with different Python versions.**
    """
    try:
        # 1. Get the authoritative column order from the DDL
        create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
        if not create_table_match:
            return sxml_string, False, [], []

        columns_block = create_table_match.group(1)
        ddl_ordered_cols = [name.upper() for name in re.findall(r'^\s*"([^"]+)"', columns_block, re.MULTILINE | re.IGNORECASE)]
        
        if not ddl_ordered_cols:
            return sxml_string, False, [], []

        # 2. Parse the SXML and get the current order and a map of elements
        ns = {'ku': 'http://xmlns.oracle.com/ku'}
        ET.register_namespace('', ns['ku'])
        
        root = ET.fromstring(sxml_string)
        col_list_element = root.find('.//ku:RELATIONAL_TABLE/ku:COL_LIST', ns)
        if col_list_element is None:
            return sxml_string, False, [], []

        sxml_col_map = {
            item.find('ku:NAME', ns).text.strip().upper(): item 
            for item in col_list_element.findall('./ku:COL_LIST_ITEM', ns)
            if item.find('ku:NAME', ns) is not None
        }
        
        current_sxml_order = [item.find('ku:NAME', ns).text.strip().upper() for item in col_list_element.findall('./ku:COL_LIST_ITEM', ns) if item.find('ku:NAME', ns) is not None]

        # 3. Compare orders and check if reordering is needed
        if len(ddl_ordered_cols) == len(current_sxml_order) and all(ddl_ordered_cols[i] == current_sxml_order[i] for i in range(len(ddl_ordered_cols))):
             return sxml_string, False, [], []

        # 4. Rebuild the COL_LIST in the correct order
        original_items = list(col_list_element)
        for item in original_items:
            col_list_element.remove(item)

        for col_name in ddl_ordered_cols:
            if col_name in sxml_col_map:
                col_list_element.append(sxml_col_map[col_name])
        
        for col_name in sxml_col_map:
            if col_name not in ddl_ordered_cols:
                col_list_element.append(sxml_col_map[col_name])

        # 5. Serialize the modified XML tree back to a string (version-safe method)
        xml_body = ET.tostring(root, encoding='unicode')
        reordered_sxml = '<?xml version="1.0" ?>\n' + xml_body
        
        return reordered_sxml, True, current_sxml_order, ddl_ordered_cols

    except (ET.ParseError, AttributeError, TypeError) as e:
        print(f"  Warning: Could not process SXML for reordering. Reason: {e}")
        return sxml_string, False, [], []


# All other functions (add_missing_columns_to_sxml, compare_ddl_and_sxml_columns, etc.)
# remain the same as the previous version. Only the function above needed a correction.


def add_missing_columns_to_sxml(missing_columns, ddl_string, sxml_string):
    new_col_items = []
    create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
    if not create_table_match:
        return sxml_string

    columns_block = create_table_match.group(1)
    
    for col_name in missing_columns:
        col_def_match = re.search(r'^\s*"' + re.escape(col_name) + r'"\s+(.*)', columns_block, re.MULTILINE | re.IGNORECASE)
        if not col_def_match:
            continue

        col_def = col_def_match.group(1).strip().rstrip(',')
        item_xml = f'      <COL_LIST_ITEM>\n        <NAME>{col_name}</NAME>\n'
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
        elif type_def_upper.startswith('TIMESTAMP'):
            item_xml += '        <DATATYPE>TIMESTAMP_WITH_LOCAL_TIMEZONE</DATATYPE>\n'
            scale_match = re.search(r'\((\d+)\)', col_def)
            if scale_match:
                item_xml += f'        <SCALE>{scale_match.group(1)}</SCALE>\n'

        if 'NOT NULL' in type_def_upper:
            item_xml += '        <NOT_NULL></NOT_NULL>\n'
            
        item_xml += '      </COL_LIST_ITEM>\n'
        new_col_items.append(item_xml)

    if new_col_items:
        col_list_end_tag = '</COL_LIST>'
        insertion_point = sxml_string.find(col_list_end_tag)
        if insertion_point != -1:
            updated_sxml = sxml_string[:insertion_point] + "".join(new_col_items) + sxml_string[insertion_point:]
            return updated_sxml

    return sxml_string


def compare_ddl_and_sxml_columns(ddl_string, sxml_string):
    messages = []
    ddl_cols = {}
    sxml_cols = {}
    
    create_table_match = re.search(r'CREATE\s+TABLE\s+.*?\((.*)\)', ddl_string, re.DOTALL | re.IGNORECASE)
    if create_table_match:
        columns_block = create_table_match.group(1)
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
                match = re.search(r'NUMBER\((\d+),(\d+)\)', definition, re.IGNORECASE)
                if match:
                    ddl_cols[name]['precision'] = match.group(1)
                    ddl_cols[name]['scale'] = match.group(2)
                else:
                    match = re.search(r'NUMBER\((\d+)\)', definition, re.IGNORECASE)
                    if match:
                        ddl_cols[name]['precision'] = match.group(1)
                        ddl_cols[name]['scale'] = '0'
            elif type_def.startswith('DATE'):
                ddl_cols[name]['type'] = 'DATE'
            elif type_def.startswith('CLOB'):
                ddl_cols[name]['type'] = 'CLOB'
            elif type_def.startswith('BLOB'):
                ddl_cols[name]['type'] = 'BLOB'
            elif type_def.startswith('TIMESTAMP'):
                ddl_cols[name]['type'] = 'TIMESTAMP_WITH_LOCAL_TIMEZONE'
                scale_match = re.search(r'\((\d+)\)', definition)
                if scale_match:
                    ddl_cols[name]['scale'] = scale_match.group(1)

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

    ddl_col_names = set(ddl_cols.keys())
    sxml_col_names = set(sxml_cols.keys())
    
    in_ddl_not_in_sxml = ddl_col_names - sxml_col_names
    in_sxml_not_in_ddl = sxml_col_names - ddl_col_names
    attribute_mismatches = []

    if in_ddl_not_in_sxml:
        messages.append(f"  Discrepancy: Columns in DDL but not in SXML -> {sorted(list(in_ddl_not_in_sxml))}")
    
    if in_sxml_not_in_ddl:
        messages.append(f"  Discrepancy: Columns in SXML but not in DDL -> {sorted(list(in_sxml_not_in_ddl))}")

    common_cols = ddl_col_names.intersection(sxml_col_names)
    for col in common_cols:
        ddl_attr = ddl_cols[col]
        sxml_attr = sxml_cols[col]
        mismatches = []
        
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

def fix_identity_not_null(sxml_string):
    id_col_match = re.search(r'(<COL_LIST_ITEM>\s*<NAME>ID</NAME>.*?)(</COL_LIST_ITEM>)', sxml_string, re.DOTALL)
    if id_col_match:
        id_col_block = id_col_match.group(1)
        if '<NOT_NULL/>' not in id_col_block and '<IDENTITY_COLUMN>' in id_col_block:
            identity_end_tag = '</IDENTITY_COLUMN>'
            insertion_point = id_col_block.find(identity_end_tag) + len(identity_end_tag)
            corrected_block = id_col_block[:insertion_point] + '\n        <NOT_NULL/>' + id_col_block[insertion_point:]
            return sxml_string.replace(id_col_block, corrected_block), "Added missing NOT NULL tag to ID column."
    return None, None

def reset_start_with_value(sxml_string):
    start_with_match = re.search(r'(<START_WITH>)(\d+)(</START_WITH>)', sxml_string)
    if start_with_match:
        original_value = start_with_match.group(2)
        if original_value != '1':
            corrected_sxml = sxml_string.replace(start_with_match.group(0), f'<START_WITH>1</START_WITH>')
            return corrected_sxml, True, original_value
    return sxml_string, False, None


def generate_log_file(file_path, ddl_content, original_sxml, corrected_sxml, discrepancies, fixes_applied, git_diff_output=None):
    """
    Generates a complete log file with all features:
    1. Summary of changes at the top.
    2. Detailed old/new order for reorder fixes.
    3. Robust parsing for potentially malformed original SXML.
    4. Appended Git diff against the 'main' branch.
    """
    try:
        log_file_path = os.path.splitext(file_path)[0] + ".log"
        with open(log_file_path, 'w', encoding='utf-8') as log_f:
            
            # Feature 1 & 2: Summary of Changes with Reorder Detail
            if fixes_applied:
                log_f.write("--- Summary of Changes ---\n")
                for fix in fixes_applied:
                    log_f.write(f"- {fix.get('message', 'An undescribed fix was applied.')}\n")
                    if fix.get('type') == 'reorder':
                        old_order_str = ", ".join(fix.get('old_order', []))
                        new_order_str = ", ".join(fix.get('new_order', []))
                        log_f.write(f"    - Original Order: {old_order_str}\n")
                        log_f.write(f"    - New Order:      {new_order_str}\n")
                log_f.write("--------------------------\n\n")

            # DDL Section
            log_f.write("--- DDL ---\n")
            log_f.write(ddl_content.strip() + "\n\n")
            
            # Feature 3: Robust "Original SXML" Printing
            log_f.write("--- Original SXML (Before) ---\n")
            try:
                # Attempt to pretty-print the XML as normal
                dom_original = minidom.parseString(original_sxml)
                ugly_xml_original = dom_original.toprettyxml(indent="  ")
                good_lines_original = [line for line in ugly_xml_original.split('\n') if line.strip()]
                formatted_sxml_original = "\n".join(good_lines_original)
                log_f.write(formatted_sxml_original + "\n\n")
            except Exception as e:
                # If parsing fails, write a note and print the raw string
                log_f.write("\n")
                log_f.write(f"\n")
                log_f.write(original_sxml + "\n\n")

            # Corrected SXML Section
            log_f.write("--- Corrected SXML (After) ---\n")
            try:
                dom_corrected = minidom.parseString(corrected_sxml)
                ugly_xml_corrected = dom_corrected.toprettyxml(indent="  ")
                good_lines_corrected = [line for line in ugly_xml_corrected.split('\n') if line.strip()]
                formatted_sxml_corrected = "\n".join(good_lines_corrected)
                log_f.write(formatted_sxml_corrected)
            except Exception as e:
                # If parsing fails, write a note and print the raw string
                log_f.write("\n")
                log_f.write(f"\n")
                log_f.write(corrected_sxml + "\n\n")

            # Feature 4: Git Diff Section
            if git_diff_output:
                log_f.write(f"\n\n--- Git Diff vs. {repo} Branch ---\n")
                log_f.write(git_diff_output)

        return f"INFO: Discrepancy details saved to: {log_file_path}"
    except Exception as e:
        return f"ERROR: Could not write log file. Reason: {e}"
    
def process_single_file(file_path, reset_start_with_flag,repo):
    snapshot_prefix = "-- sqlcl_snapshot"
    messages = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        original_line_index = -1
        # Find the snapshot line index first
        for i, line in enumerate(lines):
            if line.strip().startswith(snapshot_prefix):
                original_line_index = i
                break
        
        # If no snapshot line is found, there's nothing to process
        if original_line_index == -1:
            return

        file_was_modified = False
        fixes_applied_for_log = []
        
        json_string = lines[original_line_index].strip()[len(snapshot_prefix):].strip()
        
        if not json_string:
            messages.append(f"WARNING (Line {original_line_index + 1}): Snapshot line is empty.")
        else:
            try:
                data = json.loads(json_string)
                original_sxml = data.get("sxml")

                if not original_sxml:
                    messages.append(f"WARNING (Line {original_line_index + 1}): JSON data is missing the 'sxml' key.")
                else:
                    sxml_to_process = original_sxml
                    
                    # Attempt to parse, and fix if there's a known error
                    try:
                        ET.fromstring(sxml_to_process)
                    except ET.ParseError as xml_err:
                        corrected_sxml, fix_message = fix_identity_column(sxml_to_process)
                        if corrected_sxml:
                            sxml_to_process = corrected_sxml
                            messages.append(f"SUCCESS (Line {original_line_index + 1}): {fix_message}")
                            fixes_applied_for_log.append({'message': "Fixed missing IDENTITY_COLUMN tag."})
                            file_was_modified = True
                        else:
                            messages.append(f"ERROR (Line {original_line_index + 1}): {fix_message or f'Unfixable SXML parse error: {xml_err}'}")
                            sxml_to_process = None

                    # Proceed with fixes only if SXML is valid or was successfully fixed
                    if sxml_to_process:
                        ddl_content = "".join(lines[:original_line_index])
                        
                        initial_comp_messages, initial_in_ddl, initial_in_sxml, initial_mismatches = compare_ddl_and_sxml_columns(ddl_content, sxml_to_process)
                        
                        if initial_in_ddl:
                            sxml_to_process = add_missing_columns_to_sxml(initial_in_ddl, ddl_content, sxml_to_process)
                            msg = f"Added missing columns to SXML: {sorted(list(initial_in_ddl))}"
                            messages.append(f"SUCCESS (Line {original_line_index + 1}): {msg}")
                            fixes_applied_for_log.append({'message': f"Added missing columns: {sorted(list(initial_in_ddl))}"})
                            file_was_modified = True

                        id_not_null_mismatch = any(m['column'] == 'ID' and "NOT NULL mismatch" in ''.join(m['details']) for m in initial_mismatches)
                        if id_not_null_mismatch:
                            corrected_sxml, fix_message = fix_identity_not_null(sxml_to_process)
                            if corrected_sxml:
                                sxml_to_process = corrected_sxml
                                messages.append(f"SUCCESS (Line {original_line_index + 1}): {fix_message}")
                                fixes_applied_for_log.append({'message': "Added NOT NULL to ID column."})
                                file_was_modified = True

                        if reset_start_with_flag:
                            sxml_to_process, was_reset, old_val = reset_start_with_value(sxml_to_process)
                            if was_reset:
                                reset_message = f"Reset START_WITH value from '{old_val}' to '1'."
                                messages.append(f"SUCCESS (Line {original_line_index + 1}): {reset_message}")
                                fixes_applied_for_log.append({'message': reset_message})
                                file_was_modified = True

                        sxml_to_process, was_reordered, old_order, new_order = reorder_sxml_columns_to_match_ddl(ddl_content, sxml_to_process)
                        if was_reordered:
                            reorder_message = "Corrected SXML column order to match DDL."
                            messages.append(f"SUCCESS (Line {original_line_index + 1}): {reorder_message}")
                            fixes_applied_for_log.append({
                                'type': 'reorder',
                                'message': reorder_message,
                                'old_order': old_order,
                                'new_order': new_order
                            })
                            file_was_modified = True

                        # --- Final logic for saving, diffing, and logging ---
                        final_comp_messages, final_in_ddl, final_in_sxml, final_mismatches = compare_ddl_and_sxml_columns(ddl_content, sxml_to_process)
                        has_discrepancy = bool(final_in_ddl or final_in_sxml or final_mismatches)
                        
                        git_diff_content = None
                        
                        if file_was_modified:
                            data['sxml'] = sxml_to_process
                            lines[original_line_index] = f"-- sqlcl_snapshot {json.dumps(data, separators=(',', ':'))}\n"
                            
                            # 1. Save the modified file to disk
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.writelines(lines)
                            
                            # 2. Get the Git diff of the saved file
                        
                        git_diff_content = get_git_diff(file_path,repo)
                        is_diff_clean = (git_diff_content == "Git diff: Files are the same.")

                        # 3. Generate the comprehensive log
                        if file_was_modified or has_discrepancy or not is_diff_clean:
                            discrepancies_for_log = (final_in_ddl, final_in_sxml, final_mismatches)
                            if has_discrepancy:
                                messages.extend(final_comp_messages)
                            
                            log_message = generate_log_file(
                                file_path, ddl_content, original_sxml, sxml_to_process,
                                discrepancies_for_log, fixes_applied_for_log,
                                git_diff_output=git_diff_content
                            )
                            messages.append(f"  {log_message}")

            except json.JSONDecodeError as json_err:
                messages.append(f"ERROR (Line {original_line_index + 1}): Failed to parse JSON. Reason: {json_err}")
                
    except IOError as e:
        messages.append(f"ERROR: Could not read file. Reason: {e}")
    except Exception as e:
        messages.append(f"ERROR: An unexpected error occurred: {e}")

    if messages:
        print(f"--- Issues found in file: {file_path} ---")
        for msg in messages:
            print(f"  {msg}")
        print("-" * (len(file_path) + 25) + "\n")


def parse_sql_snapshot_files(root_folder, reset_start_with_flag,repo):
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
                process_single_file(file_path, reset_start_with_flag,repo)

if __name__ == "__main__":
    # --- IMPORTANT ---
    # Change this path to the folder you want to scan.
    # You can use a relative path (like './my_folder') or an
    # absolute path (like 'C:/Users/YourUser/Documents/sql_scripts').
    target_directory = "" 
    
       # --- OPTIONAL FLAG ---
    # Set this to True to reset all START_WITH values in identity columns to 1.
    # Set to False to leave them as they are.
    reset_start_with_flag = True
    # Set this to the branch you want to compare agains
    repo ='main'

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
    parse_sql_snapshot_files(target_directory,reset_start_with_flag,repo)
