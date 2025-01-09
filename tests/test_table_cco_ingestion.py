import pytest
import os

from sn_table_cco_ingestion_job import SNTransformer


def test_transform_full():
    input_data = [
        {
            "duration": "12 Minutes",
            "number": "OUT0160898",
            "short_description": "GoOrange Outage",
            "u_custom_export_fields": "{\"task_number.cmdb_ci\":\"\", \"task_number.short_description\":\"GoOrange - Working not possible\", \"task_number.ref_incident.u_workstation\" : \"not relevant\", \"duration_calc\" : \"1970-01-01 00:12:46\", \"duration_seconds\" : 766}",
            "cmdb_ci": "GoOrange",
            "u_affected_services": "Orange rental software| Operations App (OAPP)",
            "end": "07.11.2024 12:01:19",
            "task_number": "INC1046091",
            "type": "Outage",
            "begin": "07.11.2024 11:48:33"
        },
        {
            "duration": "3 Minutes",
            "number": "OUT0160899",
            "short_description": "Branch Network - CC 7615 Outage",
            "u_custom_export_fields": "{\"task_number.cmdb_ci\":\"7615LC2\", \"task_number.short_description\":\"Branch Monitoring - Host: 7615lc2.sixt.de - DOWN\", \"task_number.ref_incident.u_workstation\" : \"7615lc2\", \"duration_calc\" : \"1970-01-01 00:03:35\", \"duration_seconds\" : 215}",
            "cmdb_ci": "Branch Network - CC 7615",
            "u_affected_services": "",
            "end": "07.11.2024 11:53:13",
            "task_number": "INC1046113",
            "type": "Outage",
            "begin": "07.11.2024 11:49:38"
        }
    ]
    output_data =         [
        r'number,cmdb_ci,type,begin,end,task_number,task_number.cmdb_ci,task_number.short_description,task_number.ref_incident.u_workstation,duration,u_affected_services',
        r'OUT0160898,GoOrange,Outage,07.11.2024 10:48:33,07.11.2024 11:01:19,INC1046091,,GoOrange - Working not possible,not relevant,766,Orange rental software| Operations App (OAPP)',
        r'OUT0160899,Branch Network - CC 7615,Outage,07.11.2024 10:49:38,07.11.2024 10:53:13,INC1046113,7615LC2,Branch Monitoring - Host: 7615lc2.sixt.de - DOWN,7615lc2,215,'
        ]
    df = SNTransformer.transform(input_data)
    csv = df.to_csv(sep=',', index=False).rstrip(os.linesep)
    csv_lines = csv.split(os.linesep)

    assert len(csv_lines) == len(output_data)
    for line_number in range(len(csv_lines)):
            assert csv_lines[line_number] == output_data[line_number]
