from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess


class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        else:
            print(f"New file created: {event.src_path}")
            subprocess.run("elasticsearch_loader", "--index", "weather_status", "--es-host",
                           "http://elasticsearch:9200", "/parquet-data", event.src_path)


observer = Observer()
observer.schedule(NewFileHandler(), path='/parquet-data')
observer.start()
