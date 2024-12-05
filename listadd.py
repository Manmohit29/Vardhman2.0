import sqlite3

stop_category_list = ((0, "No StopType", 0),
                      (1, "Machine cleaning", 201),
                      (2, "Morning machine cleaning", 202),
                      (3, "Fabric changeover", 203),
                      (4, "Electrical Breakdown", 204),
                      (5, "Mechanical Breakdown", 205),
                      (6, "Utility Breakdown", 206),
                      (7, "Preventive Maintenance", 207),
                      (8, "Corrective Maintenance", 208),
                      (9, "Low Air Pressure", 209),
                      (10, "No Water", 210),
                      (11, "Man power shortage", 211),
                      (12, "Trolley/Batch Not Available", 212),
                      (13, "Lead cloth stop", 213),
                      (14, "No Program", 214),
                      (15, "Chemical issue", 215),
                      (16, "Pin Bar change", 216),
                      (17, "Nip Test", 217),
                      (18, "Quality fail due to mc", 218),
                      (19, "Teflon finish cleaning", 219),
                      (20, "Startup for Yardage", 220),
                      (21, "Tank Not ok", 221),
                      (22, "Quality fail due to process", 222),
                      (23, "Radiator cleaning", 223),
                      (24, "Oval Holing", 224),
                      (25, "Power Cut", 225),
                      (26, "Machine Tripping", 226),
                      (27, "Oil Dropping", 227),
                      (28, "Rubber Grinding/change", 228),
                      (29, "Temperature Problem", 229),
                      (30, "Planned Work", 230),
                      (31, "Other Faults", 231),
                      (100, "NO REASON SELECTED", 200),)

run_category = ((1, "Bulk Dyed", 130),
                (2, "Bulk Yarn Dyed", 130),
                (3, "Bulk Full Bleach", 130),
                (4, "Bulk RFD", 130),
                (5, "Reprocess", "R02"),
                (6, "Lead cloth", "L01"),
                (7, "Yardage", 130),
                (8, "BL ", 130),
                (9, "RFS", 130),
                (10, "Process demand", "R05"),
                (11, "Trial Maintenance", 224),
                (12, "RFP Bulk", 130))

Operation_list = ((0, "NA",),
                  (1, "FINISHNG",),
                  (2, "WETSTRETCH",),
                  (3, "DRYSTRETCH",),
                  (4, "DRYING",),
                  (5, "FOLDINGRTN"),
                  (6, "FULLBLEACH"),
                  (7, "POLYPAD",),
                  (8, "BSTPAD",),
                  (9, "STMPAD",),
                  (10, "REPROCESS",),
                  (11, "HEATSETT",),
                  (12, "WETHEATSET",),
                  (13, "THERMFIX",),
                  (14, "WSTRETCHLA",),
                  (15, "SANFOFINIS",),
                  (16, "FELT",),
                  (17, "DRYAIRO",),
                  (18, "AIROBEAT",),
                  (19, "CUREFINISH",))

connection = sqlite3.connect('vardhman.db')
c = connection.cursor()

#c.execute("alter table stop_category add column code TEXT")
#c.execute("alter table run_category add column code TEXT")

for i in stop_category_list:
    c.execute("UPDATE stop_category SET stop_id = ? WHERE name = ? ",(i[0],i[1]))
    #c.execute("SELECT * from stop_category where stop_id = ?", (i[0], ))
    #if c.fetchone():
    #    print(f"{i} Already exists in stop category")
    #else:
     #   c.execute("INSERT INTO stop_category (stop_id, name, code) VALUES (?, ?, ?)", i)
     #   print(f"Inserted {i}")

#for i in run_category:
#    c.execute("SELECT * from run_category where run_id = ?", (i[0], ))
#    if c.fetchone():
#        print(f"{i} Already exists in run category")
#    else:
#        c.execute("INSERT INTO run_category (run_id, name, code) VALUES (?, ?, ?)", i)
#        print(f"Inserted {i}")


connection.commit()
