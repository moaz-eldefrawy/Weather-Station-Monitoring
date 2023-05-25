from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import subprocess
import time 

class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        else:
            print(f"New file created: {event.src_path}")
            subprocess.run(["elasticsearch_loader", "--index", "weather_status", "--es-host",
                           "http://elasticsearch:9200", "parquet", event.src_path])

    
observer = PollingObserver()
observer.schedule(NewFileHandler(), path='/parquet-data')
observer.start()

try: 
  while True:
    time.sleep(1)
finally:
    observer.stop()
    observer.join()