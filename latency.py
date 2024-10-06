import matplotlib.pyplot as plt
import os

def read_latency_file(filename):
    with open(filename, 'r') as f:
        return [float(line.strip()) for line in f]

def plot_latencies():
    thread_counts = [4, 8, 16]

    # filename: configthread_{thread_num}_{thread_index}_latency.txt

    for thread_count in thread_counts:
        thread_indices = range(thread_count)
        for thread_index in thread_indices:
            filename = f"result/latency/configthread_{thread_count}_{thread_index}_latency.txt"
            if os.path.exists(filename):
                latencies = read_latency_file(filename)
                plt.plot(latencies, label=f'thread_{thread_count}_{thread_index}')
            else:
                print(f"File {filename} not found.")

    # for thread_index in thread_indices:
    #     filename = f"thread_{thread_index}_latency.txt"
    #     if os.path.exists(filename):
    #         latencies = read_latency_file(filename)
    #         plt.plot(latencies, label=f'Thread {thread_index}')
    #     else:
    #         print(f"File {filename} not found.")
    
    plt.xlabel("Transaction Index")
    plt.ylabel("Latency (ms)")
    plt.title("Transaction Latencies")
    plt.legend()
    plt.savefig("transaction_latencies.png")
    plt.show()


def plot_latencies_per_file():
    thread_counts = [4, 8, 16]

    # filename: configthread_{thread_num}_{thread_index}_latency.txt

    for thread_count in thread_counts:
        thread_indices = range(thread_count)
        for thread_index in thread_indices:
            filename = f"result/latency/configthread_{thread_count}_{thread_index}_latency.txt"
            if os.path.exists(filename):
                latencies = read_latency_file(filename)
                plt.figure()
                plt.plot(latencies, label=f'thread_{thread_count}_{thread_index}')
                plt.xlabel("Transaction Index")
                plt.ylabel("Latency (ms)")
                plt.title(f"Transaction Latencies for thread_{thread_count}_{thread_index}")
                plt.legend()
                plt.savefig(f"transaction_latencies_thread_{thread_count}_{thread_index}.png")
                plt.close()
            else:
                print(f"File {filename} not found.")

if __name__ == "__main__":
    # thread_indices = range(10)  # 根据实际线程数修改
    # plot_latencies()
    plot_latencies_per_file()