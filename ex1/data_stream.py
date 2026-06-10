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
        print("== DataStream statistics ==")

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


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("\nRegistering Numeric Processor")
    numeric_processor = NumericProcessor()
    data_stream.register_processor(numeric_processor)
    first_batch = ['Hello world', [3.14, -1, 2.71],
                   [{'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO',
                     'log_message': 'User wil is connected'}], 42, ['Hi',
                                                                    'five']]
    print(f"\nSend first batch of data on stream: {first_batch}")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("\nRegistering other data processors")
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)
    print("Send the same batch again")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("\nConsume some elements from the data processors:", end=" ")
    print("Numeric 3, Text 2, Log 1")
    for i in range(3):
        numeric_processor.output()
    for i in range(2):
        text_processor.output()
    for i in range(1):
        log_processor.output()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
