import wmi
import time
import logging
import concurrent.futures
from functools import partial


def display(result):
    # logging.info(f"Type of Avg. Disk Queue Length: {type(result[0])}")
    logging.info(f"Avg. Disk Queue Length: {result[0]}")

    # logging.info(f"Type of Disk Bytes/sec: {type(result[1])}")
    logging.info(f"Disk Bytes/sec: {result[1]}")

    # logging.info(f"Type of Available Memory (MB): {type(result[2])}")
    logging.info(f"Available Memory (MB): {result[2]}")

    # logging.info(f"Type of Page Faults/sec: {type(result[3])}")
    logging.info(f"Page Faults/sec: {result[3]}")

    # logging.info(f"Type of Bytes Total/sec: {type(result[4])}")
    logging.info(f"Bytes Total/sec: {result[4]}")

    # logging.info(f"Type of Paging File % Usage: {type(result[5])}")
    logging.info(f"Paging File % Usage: {result[5]}")

    # logging.info(f"Type of % Disk Time: {type(result[6])}")
    logging.info(f"% Disk Time: {result[6]}")

    # logging.info(f"Type of % Privileged Time: {type(result[7])}")
    logging.info(f"% Privileged Time: {result[7]}")

    # logging.info(f"Type of % Processor Time: {type(result[8])}")
    logging.info(f"% Processor Time: {result[8]}")

    # logging.info(f"Type of Context Switches/sec: {type(result[9])}")
    logging.info(f"Context Switches/sec: {result[9]}")

    # logging.info(f"Type of Processor Queue Length: {type(result[10])}")
    logging.info(f"Processor Queue Length: {result[10]}")


def performance_metrics():
    def decorator(fun_name):
        def wrapper(*args, **kwargs):
            # logging.info("Initial performance metrics")
            # initial_metrics = get_wmi_metrics()
            # display(initial_metrics)
            try:
                logging.info("*" * 90)
                start_time = time.time()
                fun_name(*args, **kwargs)
                current_metrics = get_wmi_metrics()
            except Exception as ex:
                logging.error(ex)
            else:
                end_time = time.time()
                
                elapsed_time = end_time - start_time
                logging.info(f"Time taken: {elapsed_time:.5f} seconds")
            finally:
                logging.info("*" * 90)
            display(current_metrics)
        return wrapper

    return decorator


def average_performance_metrics(total_connections):
    def decorator(fun_name):
        def wrapper(*args, **kwargs):
            # logging.info("Initial performance metrics")
            # initial_metrics = get_wmi_metrics()
            total_metrics = [0] * 11
            # display(initial_metrics)
            try:
                logging.info("*" * 90)
                start_time = time.time()
                logging.info(f"{total_connections=}")

                tasks = [
                    partial(fun_name, *args, **kwargs) for _ in range(total_connections)
                ]

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = executor.map(lambda x: (x, get_wmi_metrics()), tasks)
                for result, current_metrics in results:
                    # print(f"{result=} {current_metrics=}")
                    total_metrics = [
                        sum(x) for x in zip(total_metrics, current_metrics)
                    ]

            except Exception as ex:
                logging.error(ex)
            else:
                end_time = time.time()
                elapsed_time = end_time - start_time
                logging.info(f"Time taken: {elapsed_time:.5f} seconds")
            finally:
                logging.info("*" * 90)
            total_metrics = [x / total_connections for x in total_metrics]
            display(total_metrics)
            return total_metrics

        return wrapper

    return decorator


import pythoncom


def get_wmi_metrics():
    try:
        pythoncom.CoInitialize()
        # Connect to Windows Management Instrumentation (WMI)
        c = wmi.WMI()

        # LogicalDisk(_Total)\Avg. Disk Queue Length
        disk_queue_length = float(
            c.Win32_PerfFormattedData_PerfDisk_LogicalDisk()[0].AvgDiskQueueLength
        )

        # LogicalDisk(_Total)\Disk Bytes/sec
        disk_bytes_sec = c.Win32_PerfFormattedData_PerfDisk_LogicalDisk()[
            0
        ].DiskBytesPersec
        disk_bytes_sec = float(disk_bytes_sec) / (1024 * 1024)

        # Memory\Available Mbytes
        available_memory_mbytes = float(
            c.Win32_PerfFormattedData_PerfOS_Memory()[0].AvailableMBytes
        )

        # Memory\Page Faults/sec
        page_faults_sec = getattr(
            c.Win32_PerfFormattedData_PerfOS_Memory()[0], "PageFaultsPerSec", "N/A"
        )

        # Network Interface\Bytes Total/sec
        network_bytes_total_sec = float(
            c.Win32_PerfFormattedData_Tcpip_NetworkInterface()[0].BytesTotalPersec
        )

        # Paging File(_Total)\% Usage
        paging_file_usage = float(
            c.Win32_PerfFormattedData_PerfOS_PagingFile()[0].PercentUsage
        )

        # PhysicalDisk(_Total)\% Disk Time
        disk_time_percent = float(
            c.Win32_PerfFormattedData_PerfDisk_PhysicalDisk()[0].PercentDiskTime
        )

        # Processor(_Total)\% Privileged Time
        privileged_time_percent = float(
            c.Win32_PerfFormattedData_PerfOS_Processor()[0].PercentPrivilegedTime
        )

        # Processor(_Total)\% Processor Time
        processor_time_percent = float(
            c.Win32_PerfFormattedData_PerfOS_Processor()[0].PercentProcessorTime
        )

        # System\Context Switches/sec
        context_switches_sec = float(
            c.Win32_PerfFormattedData_PerfOS_System()[0].ContextSwitchesPerSec
        )

        # System\Processor Queue Length
        processor_queue_length = float(
            c.Win32_PerfFormattedData_PerfOS_System()[0].ProcessorQueueLength
        )

    except Exception as ex:
        logging.error("Error retrieving Windows performance metrics: " + str(ex))
        return None

    return (
        disk_queue_length,
        disk_bytes_sec,
        available_memory_mbytes,
        page_faults_sec,
        network_bytes_total_sec,
        paging_file_usage,
        disk_time_percent,
        privileged_time_percent,
        processor_time_percent,
        context_switches_sec,
        processor_queue_length,
    )


# import psutil


# def get_psutil_metrics():
#     try:
#         # LogicalDisk(_Total)\Avg. Disk Queue Length
#         disk_queue_length = psutil.disk_io_counters().read_count

#         # LogicalDisk(_Total)\Disk Bytes/sec
#         disk_bytes_sec = psutil.disk_io_counters().read_bytes

#         # Memory\Available Mbytes
#         available_memory_mbytes = psutil.virtual_memory().available / (1024 * 1024)

#         # Memory\Page Faults/sec
#         swap = psutil.swap_memory()
#         if swap.sout != 0:
#             page_faults_sec = swap.sin / swap.sout
#         else:
#             page_faults_sec = "N/A"
#         # Memory\Pages/sec
#         pages_sec = None  # psutil doesn't provide this metric directly

#         # Network Interface\Bytes Total/sec
#         network_bytes_total_sec = (
#             psutil.net_io_counters().bytes_recv + psutil.net_io_counters().bytes_sent
#         )

#         # Paging File(_Total)\% Usage
#         paging_file_usage = None  # psutil doesn't provide this metric directly

#         # PhysicalDisk(_Total)\% Disk Time
#         disk_busy_percent = psutil.disk_usage("/").percent

#         # Processor(_Total)\% Privileged Time
#         privileged_time_percent = psutil.cpu_times().system

#         # Processor(_Total)\% Processor Time
#         processor_time_percent = psutil.cpu_percent()

#         # System\Context Switches/sec
#         context_switches_sec = psutil.cpu_stats().ctx_switches

#         # System\Processor Queue Length
#         processor_queue_length = None  # psutil doesn't provide this metric directly

#         # Logging the metrics
#         logging.info(f"Avg. Disk Queue Length: {disk_queue_length}")
#         logging.info(f"Disk Bytes/sec: {disk_bytes_sec}")
#         logging.info(f"Available Memory (MB): {available_memory_mbytes}")
#         logging.info(f"Page Faults/sec: {page_faults_sec}")
#         logging.info(f"Pages/sec: {pages_sec}")
#         logging.info(f"Network Bytes Total/sec: {network_bytes_total_sec}")
#         logging.info(f"Paging File % Usage: {paging_file_usage}")
#         logging.info(f"% Disk Time: {disk_busy_percent}")
#         logging.info(f"% Privileged Time: {privileged_time_percent}")
#         logging.info(f"% Processor Time: {processor_time_percent}")
#         logging.info(f"Context Switches/sec: {context_switches_sec}")
#         logging.info(f"Processor Queue Length: {processor_queue_length}")

#     except Exception as ex:
#         logging.error("Error retrieving performance metrics: " + str(ex))


# import win32pdh
# import time


# def get_win32pdh_metrics():
#     try:
#         metrics = {
#             "Avg. Disk Queue Length": "LogicalDisk(_Total)\\Avg. Disk Queue Length",
#             "Disk Bytes/sec": "LogicalDisk(_Total)\\Disk Bytes/sec",
#             "Available Memory (MB)": "Memory\\Available Mbytes",
#             "Page Faults/sec": "Memory\\Page Faults/sec",
#             "Pages/sec": "Memory\\Pages/sec",
#             "Network Bytes Total/sec": "Network Interface\\Bytes Total/sec",
#             "Paging File % Usage": "Paging File(_Total)\\% Usage",
#             "% Disk Time": "PhysicalDisk(_Total)\\% Disk Time",
#             "% Privileged Time": "Processor(_Total)\\% Privileged Time",
#             "% Processor Time": "Processor(_Total)\\% Processor Time",
#             "Context Switches/sec": "System\\Context Switches/sec",
#         }

#         values = {}
#         for name, metric_path in metrics.items():
#             try:
#                 path = win32pdh.MakeCounterPath(
#                     (None,) + win32pdh.ParseCounterPath(metric_path)
#                 )
#                 counter_handle = win32pdh.AddCounter(None, path)
#                 win32pdh.CollectQueryData(None)
#                 time.sleep(1)
#                 win32pdh.CollectQueryData(None)
#                 _, value = win32pdh.GetFormattedCounterValue(
#                     counter_handle, win32pdh.PDH_FMT_LONG
#                 )
#                 values[name] = value
#             except Exception as e:
#                 values[name] = "N/A"

#         # Logowanie wartości metryk
#         for name, value in values.items():
#             logging.info(f"{name}: {value}")

#     except Exception as ex:
#         logging.error("Error retrieving performance metrics: " + str(ex))
