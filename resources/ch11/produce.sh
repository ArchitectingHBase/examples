#!/bin/bash
 echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>3</SIN><FirstName></FirstName><LastName></LastName></PatientRecord><MedicalRecord><Comments>This is a comment 1</Comments></MedicalRecord></ClinicalDocument>" | kafka-console-producer --topic documents --broker-list localhost:9092
sleep 10
 echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>3</SIN><FirstName></FirstName><LastName></LastName></PatientRecord><MedicalRecord><Comments>This is a comment 2</Comments></MedicalRecord></ClinicalDocument>" | kafka-console-producer --topic documents --broker-list localhost:9092
sleep 10
 echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>3</SIN><FirstName></FirstName><LastName></LastName></PatientRecord><MedicalRecord><Comments>This is a comment 3</Comments></MedicalRecord></ClinicalDocument>" | kafka-console-producer --topic documents --broker-list localhost:9092
sleep 10
 echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>3</SIN><FirstName>With First Name</FirstName><LastName>With Last Name</LastName></PatientRecord><MedicalRecord><Comments>This is a comment 4</Comments></MedicalRecord></ClinicalDocument>" | kafka-console-producer --topic documents --broker-list localhost:9092
sleep 10
 echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>3</SIN><FirstName></FirstName><LastName></LastName></PatientRecord><MedicalRecord><Comments>This is a comment 5</Comments></MedicalRecord></ClinicalDocument>" | kafka-console-producer --topic documents --broker-list localhost:9092
sleep 10
