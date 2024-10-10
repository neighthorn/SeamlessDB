from collections import defaultdict

def analyze_pin_unpin(file_path):
    pin_count = defaultdict(int)  # 用来存储每个页面的 pin 次数
    unpin_count = defaultdict(int)  # 用来存储每个页面的 unpin 次数

    with open(file_path, 'r') as file:
        for line in file:
            if '[PIN]' in line:
                # 提取 page number
                page_no = int(line.split('[PageNo: ')[1].split(']')[0])
                pin_count[page_no] += 1
            elif '[UNPIN]' in line:
                # 提取 page number
                page_no = int(line.split('[PageNo: ')[1].split('}')[0])
                unpin_count[page_no] += 1

    # 找出 pin 和 unpin 次数不相等的页面
    inconsistent_pages = []
    all_pages = set(pin_count.keys()).union(set(unpin_count.keys()))  # 所有出现过的页面

    for page in all_pages:
        if pin_count[page] != unpin_count[page]:
            inconsistent_pages.append((page, pin_count[page], unpin_count[page]))

    if inconsistent_pages:
        print("以下页面的 pin 和 unpin 次数不相等:")
        for page, pin_times, unpin_times in inconsistent_pages:
            print(f"PageNo: {page}, PIN次数: {pin_times}, UNPIN次数: {unpin_times}")
    else:
        print("所有页面的 pin 和 unpin 次数相等。")

# 使用示例，假设文件路径为 'pin_unpin_log.txt'
analyze_pin_unpin('build/storage_server_val.log')
