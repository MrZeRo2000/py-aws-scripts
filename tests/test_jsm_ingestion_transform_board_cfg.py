import pytest
import json
from jsm_ingestion_job import BoardConfiguration, ColumnConfiguration, Column, BoardConfigurationTransformer

CONFIGURATION_JSON = '''
{
    "id": 12874,
    "name": "GOLF board",
    "type": "kanban",
    "self": "https://jira.sixt.com/rest/agile/1.0/board/12874/configuration",
    "filter": {
        "id": "25420",
        "self": "https://jira.sixt.com/rest/api/2/filter/25420"
    },
    "subQuery": {
        "query": "fixVersion in unreleasedVersions() OR fixVersion is EMPTY"
    },
    "columnConfig": {
        "columns": [
            {
                "name": "Backlog",
                "statuses": [
                    {
                        "id": "10013",
                        "self": "https://jira.sixt.com/rest/api/2/status/10013"
                    }
                ]
            },
            {
                "name": "Selected for Development",
                "statuses": [
                    {
                        "id": "12617",
                        "self": "https://jira.sixt.com/rest/api/2/status/12617"
                    }
                ]
            },
            {
                "name": "In Progress",
                "statuses": [
                    {
                        "id": "3",
                        "self": "https://jira.sixt.com/rest/api/2/status/3"
                    }
                ]
            },
            {
                "name": "In Review",
                "statuses": [
                    {
                        "id": "10009",
                        "self": "https://jira.sixt.com/rest/api/2/status/10009"
                    }
                ]
            },
            {
                "name": "Done",
                "statuses": [
                    {
                        "id": "10023",
                        "self": "https://jira.sixt.com/rest/api/2/status/10023"
                    },
                    {
                        "id": "10873",
                        "self": "https://jira.sixt.com/rest/api/2/status/10873"
                    }
                ]
            }
        ],
        "constraintType": "issueCount"
    },
    "ranking": {
        "rankCustomFieldId": 10009
    }
}
'''


@pytest.fixture(scope="module")
def board_configuration() -> BoardConfiguration:
    board_json = json.loads(CONFIGURATION_JSON)
    return BoardConfiguration.model_validate_json(json.dumps(board_json))

def test_transform_columns(board_configuration: BoardConfiguration):
    transformer = BoardConfigurationTransformer(board_configuration)
    columns_dict = transformer.transform_columns()

    assert columns_dict['10009'] == 'In Review'
    assert columns_dict['10013'] == 'Backlog'

