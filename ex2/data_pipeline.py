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
            return (isinstance(data.get("log_level"), str) and
                    isinstance(data.get("log_message"), str))
        if isinstance(data, list):
            return all(isinstance(val, dict) and
                       isinstance(val.get("log_level"), str) and
                       isinstance(val.get("log_message"), str) for val in data)
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
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = []
        for _, value in data:
            values.append(value)
        print("CSV Output:")
        print(",".join(values))


class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        parts: list[str] = []

        for rank, value in data:
            parts.append(f'"item_{rank}": "{value}"')

        print("JSON Output:")
        print("{" + ", ".join(parts) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []
        self._success: bool = False

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            self._success = False
            for processor in self._processors:
                if processor.validate(item):
                    processor.ingest(item)
                    self._success = True
                    break
            if not self._success:
                print("DataStream error-", end=" ")
                print(f"Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")
            return
        for processor in self._processors:
            name = processor.__class__.__name__.replace(
                "Processor", " Processor")
            print(
                f"{name}: total {processor.get_total_processed()} "
                f"items processed, remaining "
                f"{processor.get_remaining_count()} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            plugin_data: list[tuple[int, str]] = []
            for i in range(nb):
                try:
                    output = processor.output()
                    plugin_data.append(output)
                except IndexError:
                    break
            plugin.process_output(plugin_data)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("\nRegistering Processors")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(numeric_processor)
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)
    first_batch = ['Hello world', [3.14, -1, 2.71],
                   [{'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO',
                     'log_message': 'User wil is connected'}], 42, ['Hi',
                                                                    'five']]
    print(f"\nSend first batch of data on stream: {first_batch}")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, CSVExport())
    data_stream.print_processors_stats()
    second_batch = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                    [
                        {'log_level': 'ERROR',
                         'log_message': '500 server crash'},
                        {'log_level': 'NOTICE',
                         'log_message': 'Certificate expires in 10 days'}
                      ], [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data on stream: {second_batch}")
    data_stream.process_stream(second_batch)
    data_stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, JSONExport())
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
