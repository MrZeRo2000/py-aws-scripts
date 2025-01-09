"""
Script Name: sso2aws
Author: Roman
Date: 07.01.2025
Description: Reads temporary credentials sso.txt file and saves it locally
Parameters: None
Input: "sso.txt" file with credentials from SSO web interface
Output: {user profile}/.aws/credentials
"""


import os

HOME_PATH = os.path.expanduser(os.getenv('USERPROFILE')).replace("\\", "/")
SSO_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/sso.txt")).replace("\\", "/")
CREDENTIALS_FILE_NAME = f"{HOME_PATH}/.aws/credentials"

ENVS = ['dev', 'prod']

def read_sso_file() -> list[str]:
    with open(SSO_FILE_NAME, 'r', encoding='utf-8') as f:
        return f.readlines()

def transform_lines(lines: list[str]) -> list[str]:
    result = []
    default_line_number = -1

    for line in lines:
        env = next((vl for v in line.split('-') if (vl := v.lower()) in ENVS), None)
        if env is None:
            result.append(line)
        else:
            print(f"Found {env}")
            result.append(f"[{env}]\n")
            if env == 'dev':
                default_line_number = len(result)

    # generate default from dev
    if default_line_number > -1:
        result.extend([
            '\n',
            '\n',
            '[default]\n',
        ])
        result.extend(result[default_line_number:default_line_number + 4])

    return result

def write_credentials_file(file_name: str, lines: list[str]) -> None:
    credentials_file_name = file_name

    with open(credentials_file_name, 'w') as f:
        f.writelines(lines)

    print(f"Credentials written to {credentials_file_name}")


if __name__ == '__main__':
    print("Reading SSO file...")
    sso_file_lines = read_sso_file()
    print(f"SSO file: {len(sso_file_lines)} lines read")

    transformed_lines = transform_lines(sso_file_lines)

    write_credentials_file(CREDENTIALS_FILE_NAME, transformed_lines)
