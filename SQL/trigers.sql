CREATE TRIGGER check_artist_duplicate
BEFORE INSERT ON artist
FOR EACH ROW
BEGIN
    IF EXISTS (
        SELECT 1
        FROM artist
        WHERE (first_name = NEW.first_name AND last_name = NEW.last_name)
           OR (band_name = NEW.band_name)
    ) THEN

        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Record already exist';
    END IF;
END;