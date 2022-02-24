# us_metadata

Load the us legal case metadata from https://case.law/download/bulk_exports/latest/by_jurisdiction/case_metadata/us/

(U.S. Supreme Court Meta Data from Harvard Law Library of American Constitutional Reporters) - JSON (jsonl file)

This was a question by KnowledgeShark on Libera:#mysql, and it was mildly interesting, so I coded that.

----------------------------------------------------------------------------------------------------------------------

Updated on my Fork (02.23.2022):

I have updated the MariaDB Column Data Types so there are no errors while running the Python3 loader to parse .jsonl (JSON) to SQL (MariaDB) [MySQL Fork] | Create a MariaDB database; install the required Python 3 libraries; enter your correct MariaDB credentials for the newly created Database and run the Python3 script using the following command: "python3 us_metadata_loader.py" --- Depending on how much resources you have available depends on the amount of time it takes. 

**There are approximately 1.8 Million Rows in this U.S. Constitutional Law MetaData for Jurisdiction: US/SCOTUS**

**The full SQL File can be found here:** [https://archive.org/details/harvard.-cap.-meta-data.-jurisdiction.-us-02.21.2022](url) 

**Special Thanks to Isotopp from Libera.Chat [IRC Network] #MySQL for making this possible in this way! His GitHub is found at:** [https://github.com/isotopp](url)

**If you prefer to download the SQL file complete from archive.org; run the following command:**

To import to MariaDB:

`mysql -u username -p DATABASE_NAME < path/.sql` 

Best Regards,

Brandon Kastning
TruthSword / KnowledgeShark
Sharpen Your Sword 
http://sharpenyoursword.org
#JesusChristofNazareth #LetHISPeopleGo
