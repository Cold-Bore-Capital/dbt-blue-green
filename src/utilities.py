import logging
import re
from datetime import datetime, timezone
from pathlib import Path

class Utilities:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def convert_snake_to_camel_case(snake_str):
        """
        Convert a snake_case string to camelCase.

        Args:
            snake_str (str): The string to be converted.

        Returns:
            str: The camelCase string.
        """
        components = snake_str.split('_')
        # We capitalize the first letter of each component except the first one
        # with the 'title' method and join them together.
        return components[0].lower() + ''.join(x.title() for x in components[1:])

    @staticmethod
    def get_current_utc_time() -> datetime:
        """
        Get the current UTC time.

        Mac's and the server use different time. TO ensure that the time is UTC, but without the timezone,
        this function is needed... which is annoying.

        Returns:
        - datetime: The current UTC time.
        """
        return datetime.now(timezone.utc).replace(tzinfo=None)


    @staticmethod
    def camel_to_snake(name: str) -> str:
        """
        This method uses re to convert a camelCase string to snake_case.
        Args:
            name (str): The string to be converted.

        Returns:
            str: The string converted to snake_case.
        """
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @staticmethod
    def get_path_to_launch_root():
        script_dir = Path(__file__).parent.absolute()
        script_dir_list = str(script_dir).split('/')
        root_index = script_dir_list.index('python_scripts')
        root_path =  '/'.join(script_dir_list[:root_index + 1])
        return root_path

class MockLogger:

    def __init__(self):
        pass

    def info(self, msg):
        pass

    def error(self, msg):
        pass

    def warning(self, msg):
        pass

    def debug(self, msg):
        pass


if __name__ == '__main__':
    Utilities.get_path_to_launch_root()