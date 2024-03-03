import wmi
import time
import psutil
import logging
from sqlalchemy import select


def get_initial_performance_metrics():
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        disk_io_counters = psutil.disk_io_counters()
        disk_write_bytes = disk_io_counters.write_bytes
        disk_read_bytes = disk_io_counters.read_bytes
        thread_count = psutil.cpu_count(logical=False)
        available_threads = psutil.cpu_count(logical=True)
        network_io_counters = psutil.net_io_counters()
        bytes_sent = network_io_counters.bytes_sent
        bytes_recv = network_io_counters.bytes_recv

        logging.info("Initial CPU and System Metrics:")
        logging.info(f"CPU usage: {cpu_percent:.2f}%")
        logging.info(f"Memory usage: {memory_percent:.2f}%")
        logging.info(f"Disk write bytes: {disk_write_bytes}")
        logging.info(f"Disk read bytes: {disk_read_bytes}")
        logging.info(f"Thread count: {thread_count}")
        logging.info(f"Available threads: {available_threads}")
        logging.info("Initial Network Metrics:")
        logging.info(f"Network Bytes Sent: {bytes_sent}")
        logging.info(f"Network Bytes Received: {bytes_recv}")

    except Exception as ex:
        logging.error("Error retrieving initial performance metrics: " + str(ex))


def get_windows_performance_metrics():
    try:
        # Connect to Windows Management Instrumentation (WMI)
        c = wmi.WMI()

        # LogicalDisk(_Total)\Avg. Disk Queue Length
        disk_queue_length = c.Win32_PerfFormattedData_PerfDisk_LogicalDisk()[0].AvgDiskQueueLength

        # LogicalDisk(_Total)\Disk Bytes/sec
        disk_bytes_sec = c.Win32_PerfFormattedData_PerfDisk_LogicalDisk()[0].DiskBytesPersec

        # Memory\Available Mbytes
        available_memory_mbytes = c.Win32_PerfFormattedData_PerfOS_Memory()[0].AvailableMBytes

        # Memory\Page Faults/sec
        page_faults_sec = getattr(c.Win32_PerfFormattedData_PerfOS_Memory()[0], 'PageFaultsPerSec', 'N/A')

        # Memory\Pages/sec
        pages_sec = getattr(c.Win32_PerfFormattedData_PerfOS_Memory()[0], 'PagesSec', 'N/A')

        # Network Interface\Bytes Total/sec
        network_bytes_total_sec = c.Win32_PerfFormattedData_Tcpip_NetworkInterface()[0].BytesTotalPersec

        # Paging File(_Total)\% Usage
        paging_file_usage = c.Win32_PerfFormattedData_PerfOS_PagingFile()[0].PercentUsage

        # PhysicalDisk(_Total)\% Disk Time
        disk_time_percent = c.Win32_PerfFormattedData_PerfDisk_PhysicalDisk()[0].PercentDiskTime

        # Processor(_Total)\% Privileged Time
        privileged_time_percent = c.Win32_PerfFormattedData_PerfOS_Processor()[0].PercentPrivilegedTime

        # Processor(_Total)\% Processor Time
        processor_time_percent = c.Win32_PerfFormattedData_PerfOS_Processor()[0].PercentProcessorTime

        # System\Context Switches/sec
        context_switches_sec = c.Win32_PerfFormattedData_PerfOS_System()[0].ContextSwitchesPerSec

        # System\Processor Queue Length
        processor_queue_length = c.Win32_PerfFormattedData_PerfOS_System()[0].ProcessorQueueLength

        # Logging the metrics
        logging.info(f"Avg. Disk Queue Length: {disk_queue_length}")
        logging.info(f"Disk Bytes/sec: {disk_bytes_sec}")
        logging.info(f"Available Memory (MB): {available_memory_mbytes}")
        logging.info(f"Page Faults/sec: {page_faults_sec}")
        logging.info(f"Pages/sec: {pages_sec}")
        logging.info(f"Bytes Total/sec: {network_bytes_total_sec}")
        logging.info(f"Paging File % Usage: {paging_file_usage}")
        logging.info(f"% Disk Time: {disk_time_percent}")
        logging.info(f"% Privileged Time: {privileged_time_percent}")
        logging.info(f"% Processor Time: {processor_time_percent}")
        logging.info(f"Context Switches/sec: {context_switches_sec}")
        logging.info(f"Processor Queue Length: {processor_queue_length}")

    except Exception as ex:
        logging.error("Error retrieving Windows performance metrics: " + str(ex))


def get_all_data_postgres_with_performance_metrics(table_name, postgresDB, total_connections):
    try:
        start_time = time.time()

        # Symulacja wielu połączeń i operacji SELECT dla wszystkich klientów
        for _ in range(total_connections):
            table = postgresDB.get_table(table_name)
            query = select(table)
            data = postgresDB.get_df_from_sql(query)

        end_time = time.time()
        elapsed_time = end_time - start_time

        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        disk_write_bytes = psutil.disk_io_counters().write_bytes
        disk_read_bytes = psutil.disk_io_counters().read_bytes
        thread_count = psutil.cpu_count(logical=False)
        available_threads = psutil.cpu_count(logical=True)

        logging.info(f"Time taken: {elapsed_time:.5f} seconds")
        logging.info(f"CPU usage: {cpu_percent:.2f}%")
        logging.info(f"Memory usage: {memory_percent:.2f}%")
        logging.info(f"Disk write bytes: {disk_write_bytes}")
        logging.info(f"Disk read bytes: {disk_read_bytes}")
        logging.info(f"Thread count: {thread_count}")
        logging.info(f"Available threads: {available_threads}")

        disk_queue_length = psutil.disk_io_counters().queue_length
        disk_bytes_per_sec = psutil.disk_io_counters().bytes_sent + psutil.disk_io_counters().bytes_recv
        available_memory_mbytes = psutil.virtual_memory().available / (1024 ** 2)
        page_faults_per_sec = psutil.swap_memory().sin
        pages_per_sec = psutil.swap_memory().sout
        bytes_total_per_sec = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
        paging_file_usage = psutil.swap_memory().percent
        disk_time_percent = psutil.disk_io_counters().percent
        privileged_time_percent = psutil.cpu_times_percent(interval=1).system
        processor_time_percent = psutil.cpu_percent(interval=1)

        logging.info(f"Disk Queue Length: {disk_queue_length}")
        logging.info(f"Disk Bytes/sec: {disk_bytes_per_sec}")
        logging.info(f"Available Memory (MB): {available_memory_mbytes:.2f}")
        logging.info(f"Page Faults/sec: {page_faults_per_sec}")
        logging.info(f"Pages/sec: {pages_per_sec}")
        logging.info(f"Network Bytes Total/sec: {bytes_total_per_sec}")
        logging.info(f"Paging File Usage: {paging_file_usage:.2f}%")
        logging.info(f"% Disk Time: {disk_time_percent:.2f}%")
        logging.info(f"% Privileged Time: {privileged_time_percent:.2f}%")
        logging.info(f"% Processor Time: {processor_time_percent:.2f}%")
        
        return data

    except Exception as ex:
        logging.error("Cannot retrieve data from PostgreSQL: " + str(ex))
        return None


def get_all_data_mongodb_with_performance_metrics(collection_name, mongodb):
    try:
        start_time = time.time()

        data = mongodb.get_df_from_mongo(collection_name)

        end_time = time.time()
        elapsed_time = end_time - start_time

        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        disk_write_bytes = psutil.disk_io_counters().write_bytes
        disk_read_bytes = psutil.disk_io_counters().read_bytes
        thread_count = psutil.cpu_count(logical=False)
        available_threads = psutil.cpu_count(logical=True)

        logging.info(f"Time taken: {elapsed_time:.5f} seconds")
        logging.info(f"CPU usage: {cpu_percent:.2f}%")
        logging.info(f"Memory usage: {memory_percent:.2f}%")
        logging.info(f"Disk write bytes: {disk_write_bytes}")
        logging.info(f"Disk read bytes: {disk_read_bytes}")
        logging.info(f"Thread count: {thread_count}")
        logging.info(f"Available threads: {available_threads}")

        return data
    except Exception as ex:
        logging.error("Cannot retrieve data from MongoDB: " + str(ex))
        return None