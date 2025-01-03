import pytest
import os

from sn_table_sao_ingestion_job import SNTransformer


@pytest.mark.parametrize(
    'input_date,expected_date', [
        ('2024-08-01 07:00:00', '01.08.2024 05:00:00'),
        ('2024-12-06 07:00:00', '06.12.2024 06:00:00'),
    ]
)
def test_transform_convert_date(input_date: str, expected_date: str):
    assert SNTransformer.convert_date_str(input_date) == expected_date


@pytest.mark.parametrize(
    'input_date,duration', [
        ('1970-01-01 00:00:00', '0'),
        ('1970-01-01 00:00:01', '1'),
        ('1970-01-01 01:05:34', '3934'),
    ]
)
def test_transform_date_to_duration(input_date: str, duration: str):
    assert SNTransformer.date_to_duration(input_date) == duration

def test_transform_full():
    input_data = [{
            "scheduled_downtime": "1970-01-01 00:07:06",
            "scheduled_availability": "99.34259",
            "absolute_downtime": "1970-01-01 00:07:06",
            "service_commitment": {
                "link": "https://sixtprod.service-now.com/api/now/table/service_commitment/3f24e70793ea7d1036e475518bba1053",
                "value": "3f24e70793ea7d1036e475518bba1053"
            },
            "start": "2024-08-30 22:00:00",
            "end": "2024-08-31 22:00:00",
            "type": "daily",
            "display_name": "Counter Service - CC 42874 | Counter Service Availability - CC 42874",
            "service_offering": {
                "link": "https://sixtprod.service-now.com/api/now/table/service_offering/f324e70793ea7d1036e475518bba1051",
                "value": "f324e70793ea7d1036e475518bba1051"
            }
        },
        {
            "scheduled_downtime": "1970-01-01 00:07:43",
            "scheduled_availability": "99.28549",
            "absolute_downtime": "1970-01-01 00:07:43",
            "service_commitment": {
                "link": "https://sixtprod.service-now.com/api/now/table/service_commitment/3f24e70793ea7d1036e475518bba1053",
                "value": "3f24e70793ea7d1036e475518bba1053"
            },
            "start": "2024-07-30 22:00:00",
            "end": "2024-07-31 22:00:00",
            "type": "daily",
            "display_name": "Counter Service - CC 42874 | Counter Service Availability - CC 42874",
            "service_offering": {
                "link": "https://sixtprod.service-now.com/api/now/table/service_offering/f324e70793ea7d1036e475518bba1051",
                "value": "f324e70793ea7d1036e475518bba1051"
            }
        }]
    output_data =         [
            'start,end,type,service_offering,service_commitment,allowed_downtime,scheduled_downtime,scheduled_availability',
            '30.08.2024 20:00:00,31.08.2024 20:00:00,Daily,Counter Service - CC 42874,Counter Service Availability - CC 42874,426,426,99.34259',
            '30.07.2024 20:00:00,31.07.2024 20:00:00,Daily,Counter Service - CC 42874,Counter Service Availability - CC 42874,463,463,99.28549'
        ]
    df = SNTransformer.transform(input_data)
    csv = df.to_csv(sep=',', index=False).rstrip(os.linesep)

    assert csv == os.linesep.join(output_data)