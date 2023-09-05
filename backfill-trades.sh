#!/bin/bash
insert into trades from infile '2006-05-29.csv.zst' settings format_csv_delimiter='|' format CSVWithNames
