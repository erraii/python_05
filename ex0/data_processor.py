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
        return (self._data_str.pop(0))


class NumericProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        try:
            if isinstance(data, (int, float)):
                return True
            elif all(isinstance(val, (int | float)) for val in data):
                return True
            else:
                return False
        except Exception:
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
        try:
            if isinstance(data, str):
                return True
            elif all(isinstance(val, str) for val in data):
                return True
            else:
                return False
        except Exception:
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
        try:
            if isinstance(data, dict):
                return True
            elif all(isinstance(val, dict) for val in data):
                return True
            else:
                return False
        except Exception:
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


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print("\nTesting Numeric Processor...")
    numProcess = NumericProcessor()
    data_int = 42
    success = numProcess.validate(data_int)
    print(f" Trying to validate input '{data_int}': {success}")
    data_str = "Hello"
    success = numProcess.validate(data_str)
    print(f" Trying to validate input '{data_str}': {success}")
    data_str = "foo"
    print(" Test invalid ingestion of string ", end="")
    print(f"'{data_str}' without prior validation:")
    try:
        numProcess.ingest(data_str)
    except Exception as e:
        print(f" Got exception: {e}")
    int_list = [1, 2, 3, 4, 5]
    if numProcess.validate(int_list):
        print(f" Processing data: {int_list}")
        numProcess.ingest(int_list)
    print(" Extracting 3 values...")
    for i in range(0, 3):
        output = numProcess.output()
        print(f" Numeric value {output[0]}: {output[1]}")
    print("\nTesting Text Processor...")
    textProcess = TextProcessor()
    data_int = 42
    success = textProcess.validate(data_int)
    print(f" Trying to validate input '{data_int}': {success}")
    str_list = ["Hello", "Nexus", "World"]
    if textProcess.validate(str_list):
        print(f" Processing data: {str_list}")
        textProcess.ingest(str_list)
    print(" Extracting 1 value...")
    for i in range(0, 1):
        output = textProcess.output()
        print(f" Text value {output[0]}: {output[1]}")
    logProcess = LogProcessor()
    data_str = "Hello"
    success = logProcess.validate(data_str)
    print(f" Trying to validate input '{data_str}': {success}")
    dict_list = [{'log_level': 'NOTICE',
                  'log_message': 'Connection to server'},
                 {'log_level': 'ERROR',
                  'log_message': 'Unauthorized access!!'}]
    if logProcess.validate(dict_list):
        print(f" Processing data: {dict_list}")
        logProcess.ingest(dict_list)
    print(" Extracting 2 values...")
    for i in range(0, 2):
        output = logProcess.output()
        print(f" Log entry {output[0]}: {output[1]}")


if __name__ == "__main__":
    main()
