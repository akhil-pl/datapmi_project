SELECT
	person.id AS person_id,
	person.name AS person_name,
	person.age AS person_age,
	contact.id AS contact_id,
	contact.address AS contact_address,
	contact.phone AS contact_phone
FROM person
CROSS JOIN contact