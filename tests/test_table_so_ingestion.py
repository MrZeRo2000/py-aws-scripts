import pytest
import os

from sn_table_so_ingestion_job import SNTransformer


def test_transform_full():
    input_data = [
        {
            "parent": "Classic Platform",
            "support_group": "",
            "busines_criticality": "2 - High",
            "operational_status": "Operational",
            "managed_by": "",
            "cost_center": "SIXT SE",
            "name": "Classic Platform - SIXT SE",
            "used_for": "Production",
            "owned_by": "Michael Mellenthin"
        },
        {
            "parent": "Orange rental software",
            "support_group": "",
            "busines_criticality": "2 - High",
            "operational_status": "Operational",
            "managed_by": "Gaurav Khanna",
            "cost_center": "40860",
            "name": "Orange rental software - CC 40860",
            "used_for": "Production",
            "owned_by": "Tarun Dhawan"
        }
    ]
    output_data =         [
        'name,parent.service_classification,parent,portfolio_status,service_status,owned_by,delivery_manager,support_group,cost_center',
        'Classic Platform - SIXT SE,2 - High,Classic Platform,Production,Operational,Michael Mellenthin,,,SIXT SE',
        'Orange rental software - CC 40860,2 - High,Orange rental software,Production,Operational,Tarun Dhawan,Gaurav Khanna,,40860'
        ]
    df = SNTransformer.transform(input_data)
    csv = df.to_csv(sep=',', index=False).rstrip(os.linesep)

    assert csv == os.linesep.join(output_data)