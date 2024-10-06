import os


def extract_lines(log_file):
    """Extract the specified lines from the log file."""
    lines_to_extract = [
        "time for read state:",
        "time for recover state:",
        "time for recover consistency:",
        "time for re_run:",
        "time for reconnect:",
        "total srp:"
    ]
    
    extracted_lines = []
    with open(log_file, 'r') as file:
        for line in file:
            if any(phrase in line for phrase in lines_to_extract):
                extracted_lines.append(line.strip())
    
    return extracted_lines


theta_range = ['0.000000', '0.100000', '0.500000']
block_range = [100, 500, 900]

# 设置要遍历的目录
directory = '../build'

f = open('extract_results.txt', 'w')
for theta in theta_range:
    for block_size in block_range:
        
        f.write(f'Theta: {theta}, Block Size: {block_size}\n')
        
        src_file_start_with = f'rw_server_{theta}_{block_size}'
        for root, dirs, files in os.walk(directory):
            for file in files:
                # 检查文件名是否以开头
                if file.startswith(src_file_start_with):
                    print(file)
                    lines = extract_lines(os.path.join(root, file))
                    for line in lines:
                        f.write(line + '\n')
                    f.write('\n')
f.close()
                    
                        
                    

