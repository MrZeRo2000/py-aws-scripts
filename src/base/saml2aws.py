from subprocess import run
import json
import os


PRINCIPAL_ARN = "arn:aws:iam::488115367344:saml-provider/ADFS"
ROLE_ARN = "arn:aws:iam::488115367344:role/ADFS-BI-Prod-DataDeveloper"
ASSUME_ROLE_ARN = "arn:aws:iam::973209441745:role/ADFS-BI-Prod-DataDeveloper"

ROLE_ARNS = {
    "default": {
        "ROLE_ARN": "arn:aws:iam::488115367344:role/ADFS-BI-Dev-DataDeveloper",
        "ASSUME_ROLE_ARN": "arn:aws:iam::199480941921:role/ADFS-BI-Dev-DataDeveloper"
    },
    "dev": {
        "ROLE_ARN": "arn:aws:iam::488115367344:role/ADFS-BI-Dev-DataDeveloper",
        "ASSUME_ROLE_ARN": "arn:aws:iam::199480941921:role/ADFS-BI-Dev-DataDeveloper"
    },
    "prod": {
        "ROLE_ARN": "arn:aws:iam::488115367344:role/ADFS-BI-Prod-DataDeveloper",
        "ASSUME_ROLE_ARN": "arn:aws:iam::973209441745:role/ADFS-BI-Prod-DataDeveloper"
    },
}

HOME_PATH = os.path.expanduser(os.getenv('USERPROFILE')).replace("\\", "/")
SAML_ASSERT_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/saml-assert.txt")).replace("\\", "/")
CREDENTIALS_FILE_NAME = f"{HOME_PATH}/.aws/credentials"


def get_credentials_section(section_name: str, obj: dict) -> list[str]:
    credentials = [f"[{section_name}]"]
    credentials.append(f"aws_access_key_id = {obj['AccessKeyId']}")
    credentials.append(f"aws_secret_access_key = {obj['SecretAccessKey']}")
    credentials.append(f"aws_session_token = {obj['SessionToken']}")

    return credentials


def write_credentials_file(file_name: str, lines: list[str]) -> None:
    credentials_file_name = file_name

    with open(credentials_file_name, 'w') as f:
        f.write("\n".join(lines))

    print(f"Credentials written to {credentials_file_name}")


def write_credentials(obj):
    credentials = ["[default]"]
    credentials.append(f"aws_access_key_id = {obj['AccessKeyId']}")
    credentials.append(f"aws_secret_access_key = {obj['SecretAccessKey']}")
    credentials.append(f"aws_session_token = {obj['SessionToken']}")

    credentials_file_name = f"{HOME_PATH}/.aws/credentials"

    with open(credentials_file_name, 'w') as f:
        f.write("\n".join(credentials))

    print(f"Credentials written to {credentials_file_name}")


def run_command(command: str) -> tuple:
    data = run(command, capture_output=True, shell=True)
    output = [d.decode('utf-8') for d in data.stdout.splitlines() if len(d) > 0]
    errors = [d.decode('utf-8') for d in data.stderr.splitlines() if len(d) > 0]

    return data.returncode, output, errors


credentials_lines = []
for section in ROLE_ARNS:
    print(f"Running {section}")

    CMD_SAML = f"aws sts assume-role-with-saml --principal-arn {PRINCIPAL_ARN} --role-arn {ROLE_ARNS[section]['ROLE_ARN']} --saml-assertion file://{SAML_ASSERT_FILE_NAME}"
    CMD_ROLE = f"aws sts assume-role --role-arn {ROLE_ARNS[section]['ASSUME_ROLE_ARN']} --role-session-name {section}"

    saml_code, saml_output, saml_errors = run_command(CMD_SAML)

    if saml_code != 0:
        print("Error getting SAML data:" + "".join(saml_errors))
    else:
        saml_obj = json.loads("".join(saml_output))["Credentials"]
        write_credentials_file(CREDENTIALS_FILE_NAME, get_credentials_section('default', saml_obj))

        role_code, role_output, role_errors = run_command(CMD_ROLE)
        if role_code != 0:
            print("Error assuming role:" + "".join(role_errors))
        else:
            role_obj = json.loads("".join(role_output))["Credentials"]
            if len(credentials_lines) > 0:
                credentials_lines.append('')
            credentials_lines.extend(get_credentials_section(section, role_obj))
            print(f"Successfully completed section {section}")

if len(credentials_lines) > 0:
    write_credentials_file(CREDENTIALS_FILE_NAME, credentials_lines)
    print(f"Successfully completed all sections")
else:
    print("No credentials")

'''
CMD_SAML = f"aws sts assume-role-with-saml --principal-arn {PRINCIPAL_ARN} --role-arn {ROLE_ARN} --saml-assertion file://{SAML_ASSERT_FILE_NAME}"
CMD_ROLE = f"aws sts assume-role --role-arn {ASSUME_ROLE_ARN} --role-session-name prod"

saml_code, saml_output, saml_errors = run_command(CMD_SAML)

if saml_code != 0:
    print("Error getting SAML data:" + "".join(saml_errors))
else:
    saml_obj = json.loads("".join(saml_output))["Credentials"]
    write_credentials(saml_obj)
    
    role_code, role_output, role_errors = run_command(CMD_ROLE)
    if role_code != 0:
        print("Error assuming role:" + "".join(role_errors))
    else:
        role_obj = json.loads("".join(role_output))["Credentials"]
        write_credentials(role_obj)
        print("Successfully completed")
'''