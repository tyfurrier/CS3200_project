from typing import List, Set

from colorama import Fore, Style


class AtScaleExtrasDependencyImportError(Exception):
    def __init__(self, extras_type: str, nested_error: str):
        message = (f'{nested_error}\nYou may need run {Style.BRIGHT + Fore.GREEN}pip '
                   f'install \'atscale[{extras_type}]\'{Style.RESET_ALL}"')
        super().__init__(message)

class UserError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

