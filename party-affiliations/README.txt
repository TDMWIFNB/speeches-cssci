This folder contains two datasets related to Dutch political figures:

    List of Tweede Kamerleden (2006 - Present) – Data sourced from Wikipedia.
    Ministers and Staatssecretarissen (1945 - Present) – Data sourced from Parlement.com.

File Descriptions

1. List_of_Tweede_Kamerleden_2006-present.csv

This file contains information about members of the Tweede Kamer from 2006 to the present.

Columns:

    full_name – The full name of the member.
    last_name – The last name of the member.
    party – The political party they represent.
    start_date – The date when the member began their term.
    end_date – The date when the member's term ended (if applicable).


2. Ministers_Staassecretarissen_1945-present.csv

This file contains information about ministers and staatssecretarissen in the Netherlands from 1945 to the present.

Columns:

    full_name – The full name of the individual.
    last_name – The last name of the individual.
    party – The political party they were affiliated with at the time.
    function – Either "Minister" or "Staatssecretaris".
    role – The specific role, which can be:
        "Minister-president" (Prime Minister)
        "Viceminister-president" (Deputy Prime Minister)
        The department to which the minister or staatssecretaris was assigned
    cabinet – The name of the cabinet they served in.
    start_date – The date they started their position.
    end_date – The date they ended their position (if applicable).

Notes

    Dates are in Dutch format, as they match those used in the official transcripts of the Tweede Kamer minutes.
    Some entries may have missing end_date values, indicating the person is still in office.
    Party affiliations may change over time, so a person may appear multiple times under different parties.
    Individuals may also be listed multiple times due to temporary absences, such as sick leave (ziekenverlof) or parental leave (zwangerschapsverlof).