CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_time_start DATETIME, @batch_time_end DATETIME; 
	BEGIN TRY ---- SQL will run the try block if it fails run the catch to handle errors

		print'========================================';
		print'Loading Bronze layer';
		print'========================================';


		print'----------------------------------------';
		print'Loading CRM Table';
		print'----------------------------------------';

		SET @batch_time_start = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading

		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading
		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading
		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'

		print'----------------------------------------';
		print'Loading ERP Table';
		print'----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading
		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading
		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating table:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\aboza\Desktop\Pizza Sales Images\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,  --AS first row is for column name
			FIELDTERMINATOR = ',',   --- DELIMITER 
			TABLOCK --- TO ENHANCE performance it will lock table while loading

		)
		SET @end_time = GETDATE();
		PRINT'>> load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------------------------'
		SET @batch_time_end = GETDATE();
		PRINT'>> Total Batch loading duration' + CAST(datediff(second,@batch_time_start, @batch_time_end) AS NVARCHAR) + (' seconds')
	END TRY 
	BEGIN CATCH
		PRINT'============================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'============================================';
	END CATCH 
END 
