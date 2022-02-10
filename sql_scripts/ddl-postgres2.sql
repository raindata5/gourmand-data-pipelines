ALTER TABLE "_Production".business ADD COLUMN ValidTo timestamp(6) without time zone NULL
update "_Production".business set ValidTo = cast('9999-12-31' as timestamp(6) without time zone)


ALTER TABLE "_Production".businesscategory ADD COLUMN ValidTo timestamp(3) without time zone NULL
update "_Production".businesscategory set validto = cast('9999-12-31' as timestamp(3) without time zone)

-- ALTER TABLE "_Production".businesscategory ADD COLUMN ValidTo timestamp(3) without time zone NULL
-- update "_Production".businesscategory set validto = cast('9999-12-31' as timestamp(3) without time zone)

ALTER TABLE "_Production".businesscategorybridge ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".businesscategorybridge set validto = cast('9999-12-31' as timestamp(3) without time zone)


ALTER TABLE "_Production".businesstransactionbridge ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".businesstransactionbridge set validto = cast('9999-12-31' as timestamp(3) without time zone)

ALTER TABLE "_Production".event ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".event set validto = cast('9999-12-31' as timestamp(3) without time zone)

ALTER TABLE "_Production".review ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".review set validto = cast('9999-12-31' as timestamp(3) without time zone)

ALTER TABLE "_Production".transactiontype ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".transactiontype set validto = cast('9999-12-31' as timestamp(3) without time zone)

ALTER TABLE "_Production".user ADD COLUMN ValidTo timestamp(3) without time zone NULL;
update "_Production".user set validto = cast('9999-12-31' as timestamp(3) without time zone)
