import pytest
import json
from dataclasses import dataclass
from jsm_ingestion_job import Issue, History, HistoryItem, HistoryAuthor, WorkflowTransition, IssueTransformer

ISSUE_JSON = '''
{
  "id": 883771,
  "board_id": 12874,
  "key": "GOLF-370",
  "fields": {
    "summary": "Fraud SideCar pod is restarting constantly",
    "story_points": null,
    "labels": [],
    "components": [],
    "issue_type": {
      "name": "Bug"
    },
    "flagged": null,
    "resolution_date": "2025-02-13T10:24:48.000+0000",
    "resolution": {
      "name": "Fixed"
    },
    "status": {
      "name": "Done"
    },
    "environment": {
      "value": "Production"
    },
    "created": "2025-02-13T09:24:30.000+0000",
    "project": {
      "key": "GOLF"
    },
    "epic_key": "GOLF-264",
    "epic_name": "Fraud Projects (reservations and overdues)",
    "priority": {
      "name": "Medium",
      "id": "10000"
    },
    "fix_versions": []
  },
  "changelog": {
    "histories": [
      {
        "created": "2025-02-13T09:27:31.381+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "Rank",
            "fromString": "",
            "toString": "Ranked higher",
            "from": "",
            "to": ""
          }
        ]
      },
      {
        "created": "2025-02-13T09:27:35.014+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "status",
            "fromString": "Backlog",
            "toString": "Selected for Development",
            "from": "10013",
            "to": "12617"
          }
        ]
      },
      {
        "created": "2025-02-13T09:27:35.699+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "Rank",
            "fromString": "",
            "toString": "Ranked lower",
            "from": "",
            "to": ""
          }
        ]
      },
      {
        "created": "2025-02-13T09:27:40.404+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "status",
            "fromString": "Selected for Development",
            "toString": "In Progress",
            "from": "12617",
            "to": "3"
          }
        ]
      },
      {
        "created": "2025-02-13T10:24:43.348+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "status",
            "fromString": "In Progress",
            "toString": "In Review",
            "from": "3",
            "to": "10009"
          }
        ]
      },
      {
        "created": "2025-02-13T10:24:48.620+0000",
        "author": {
          "display_name": "Jesus Octavio Martinez Bermudez"
        },
        "items": [
          {
            "field": "resolution",
            "fromString": null,
            "toString": "Fixed",
            "from": null,
            "to": "1"
          },
          {
            "field": "status",
            "fromString": "In Review",
            "toString": "Done",
            "from": "10009",
            "to": "10023"
          }
        ]
      },
      {
        "created": "2025-01-29T11:07:18.263+0000",
        "author": {
          "display_name": "Raghav Saboo"
        },
        "items": [
          {
            "field": "Sprint",
            "fromString": null,
            "toString": "RDRI 226",
            "from": null,
            "to": "30034"
          }
        ]
      },
      {
        "created": "2025-02-12T09:38:31.833+0000",
        "author": {
          "display_name": "Deepak Dubey"
        },
        "items": [
          {
            "field": "Sprint",
            "fromString": "RDRI 226",
            "toString": "RDRI - 227",
            "from": "30034",
            "to": "30124"
          }
        ]
      }            
    ]
  }
}
'''


def test_check_issue_history(issue: Issue):
    history = issue.change_log.histories
    assert history is not None
    assert len(history) == 6 + 2
    assert history[0].items[0].field == 'Rank'
    assert history[0].items[0].to_string == 'Ranked higher'
    assert history[-3].items[-1].field == 'status'
    assert history[-3].items[-1].from_string == 'In Review'
    assert history[-3].items[-1].to_string == 'Done'

@pytest.fixture(scope="module")
def issue() -> Issue:
    issue_json = json.loads(ISSUE_JSON)
    pass
    return Issue.model_validate_json(json.dumps(issue_json))

def test_transform_issue(issue: Issue):
    workflow_history = IssueTransformer.transform_status_history(issue)

    assert workflow_history[0].name == "Backlog"
    assert workflow_history[0].from_date == "2025-02-13T09:24:30.000+0000"
    assert workflow_history[0].to_date == "2025-02-13T09:27:35.014+0000"

    assert workflow_history[-1].name == "Done"
    assert workflow_history[-1].from_date == "2025-02-13T10:24:48.620+0000"
    assert workflow_history[-1].to_date is None

def test_transform_sprints(issue: Issue):
    sprint_history = IssueTransformer.transform_sprints(issue)

    assert sprint_history.active_sprint == "RDRI - 227"
    assert len(sprint_history.history) == 2

    assert 30034 in sprint_history.history
    assert 30124 in sprint_history.history

    issue_sprint_history = IssueTransformer.transform_issues_sprints([issue])

    assert issue_sprint_history[0].get(issue.id) is not None

    assert issue_sprint_history[0].get(issue.id).active_sprint == "RDRI - 227"
    assert len(issue_sprint_history[0].get(issue.id).history) == 2
