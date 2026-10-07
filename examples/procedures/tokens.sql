SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Consume a one-time token: 1 if this call took it, 0 if it was already gone.
CREATE OR ALTER PROCEDURE dbo.consume
    @token NVARCHAR(200),
    @taken BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM tokens WHERE [key] = @token)
        INSERT INTO tokens VALUES (@token);
    SET @taken = @@ROWCOUNT;
    IF @taken = 0
        RETURN 1;
    RETURN 0;
END
GO

CREATE PROCEDURE dbo.issue @token NVARCHAR(200), @ttl INT = 3600 AS
BEGIN
    IF @token IS NULL OR @token = ''
        THROW 50001, N'a token is required', 1;
    INSERT INTO issued ([key], ttl) VALUES (@token, @ttl);
    PRINT N'issued ' + @token;
END
GO

CREATE PROCEDURE dbo.issue_and_consume @token NVARCHAR(200) AS
BEGIN
    DECLARE @taken BIT, @rc INT;
    EXEC dbo.issue @token;
    EXEC @rc = dbo.consume @token, @taken OUTPUT;
    SELECT @taken AS taken, @rc AS rc;
END
