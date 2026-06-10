import typing
import abc


class DataProcessor(abc.ABC):

    def __init__(self) -> None:
        self._data_str: list[tuple[int, str]] = []
        self._rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data_str:
            raise IndexError("No data to output")
        return self._data_str.pop(0)
    
    def get_total_processed(self) -> int:
        return self._rank

    def get_remaining_count(self) -> int:
        return len(self._data_str)


class NumericProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(val, (int, float)) for val in data)
        return False

    def ingest(self, data: int | float | list[int] |
               list[float] | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        else:
            if isinstance(data, list):
                for x in data:
                    self._data_str.append((self._rank, str(x)))
                    self._rank += 1
            else:
                self._data_str.append((self._rank, str(data)))
                self._rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(val, str) for val in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        else:
            if isinstance(data, list):
                for x in data:
                    self._data_str.append((self._rank, x))
                    self._rank += 1
            else:
                self._data_str.append((self._rank, data))
                self._rank += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return (isinstance(data.get("log_level"), str)
                and isinstance(data.get("log_message"), str))
        if isinstance(data, list):
            return all(isinstance(val, dict)
                and isinstance(val.get("log_level"), str)
                and isinstance(val.get("log_message"), str)
                for val in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        else:
            if isinstance(data, list):
                for x in data:
                    to_str = x["log_level"] + ": " + x["log_message"]
                    self._data_str.append((self._rank, to_str))
                    self._rank += 1
            else:
                to_str = data["log_level"] + ": " + data["log_message"]
                self._data_str.append((self._rank, to_str))
                self._rank += 1

class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]])-> None:
        print("None")

class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = []
        for _, value in data:
            values.append(value)

        print("CSV Output:")
        print(",".join(values))

class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = {}
        for _, value in data:
            key = "item_" + str(value[0])
            values.update(key, value[1])

        print("JSON Output:")
        print(values)

class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any])-> None:
        for processor in self.processors:
            for item in stream:
                if processor.validate(item):
                    processor.ingest(item)
                else:
                    print(f"DataStream error- Can't process element in stream: {item}")
                 
    def print_processors_stats(self)-> None:
        print("== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for processor in self.processors:
            name = processor.__class__.__name__.replace(
                "Processor",
                " Processor"
            )

            print(
                f"{name}: total {processor.get_total_processed()} "
                f"items processed, remaining "
                f"{processor.get_remaining_count()} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin)-> None:
        try:
            for processor in self.processors:
                plugin_data = []
                for i in range(0, nb):
                    plugin_data.append(processor.output())
                plugin.process_output(plugin_data)
        except IndexError as e:
            print(f" Got exception: {e}")





def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("Registering Numeric Processor")
    numeric_processor = NumericProcessor()
    data_stream.register_processor(numeric_processor)
    first_batch = ['Hello world', [3.14,-1, 2.71], 
                   [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 
                    42, ['Hi', 'five']]
    print("Send first batch of data on stream: ")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("Registering other data processors")
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)
    print("Send the same batch again: ")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1")
    try:
        for i in range(0, 3):
            numeric_processor.output()
    except IndexError as e:
        print(f" Got exception: {e}")
    try:
        for i in range(0, 2):
            text_processor.output()
    except IndexError as e:
        print(f" Got exception: {e}")
    try:
        for i in range(0, 1):
            log_processor.output()
    except IndexError as e:
        print(f" Got exception: {e}")
    data_stream.print_processors_stats()
    

if __name__ == "__main__":
    main()
