import re

def remove_invalid_chars(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        content = file.read()

    # Define a regex pattern for invalid XML characters
    invalid_xml_chars = re.compile(
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'
    )

    # Remove invalid XML characters
    cleaned_content = invalid_xml_chars.sub('', content)

    # Save the cleaned content back to the file or a new file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(cleaned_content)

if __name__ == "__main__":
    # Replace 'yourfile.xml' with the path to your XML file
    remove_invalid_chars('perf.svg')