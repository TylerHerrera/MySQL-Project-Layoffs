-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/tmthyjames/nashville-housing-data

-- Create raw data table.
DROP TABLE IF EXISTS `Nashville_Housing`;
CREATE TABLE `Nashville_Housing`
(`UniqueID` int DEFAULT NULL,
`ParcelID` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`LandUse` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`PropertyAddress` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`SalePrice` int DEFAULT NULL,
`SaleDate` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`LegalReference` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`SoldAsVacant` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`OwnerName` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`OwnerAddress` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`Acreage` numeric DEFAULT NULL,
`TaxDistrict` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
`LandValue` int DEFAULT NULL,
`BuildingValue` int DEFAULT NULL,
`TotalValue` int DEFAULT NULL,
`YearBuilt` int DEFAULT NULL,
`Bedrooms` int DEFAULT NULL,
`FullBath` int DEFAULT NULL,
`HalfBath` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SELECT *
FROM housing.Nashville_Housing;

LOAD DATA LOCAL INFILE '/Users/thanh/Desktop/Projects/housing_data.csv'
INTO TABLE housing.Nashville_Housing
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(UniqueID,ParcelID,LandUse,PropertyAddress,SaleDate,SalePrice,LegalReference,SoldAsVacant,OwnerName,OwnerAddress,Acreage,TaxDistrict,LandValue,BuildingValue,TotalValue,YearBuilt,Bedrooms,FullBath,HalfBath);

-- Create staging table to manage data without affecting raw data table.
CREATE TABLE `staging` (
  `UniqueID` int DEFAULT NULL,
  `ParcelID` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `LandUse` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `PropertyAddress` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `SalePrice` int DEFAULT NULL,
  `SaleDate` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `LegalReference` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `SoldAsVacant` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `OwnerName` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `OwnerAddress` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `Acreage` decimal(10,0) DEFAULT NULL,
  `TaxDistrict` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `LandValue` int DEFAULT NULL,
  `BuildingValue` int DEFAULT NULL,
  `TotalValue` int DEFAULT NULL,
  `YearBuilt` int DEFAULT NULL,
  `Bedrooms` int DEFAULT NULL,
  `FullBath` int DEFAULT NULL,
  `HalfBath` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SELECT *
FROM staging
ORDER BY ParcelID;

-- 1. Remove duplicates.

-- Check for duplicates
WITH duplicate_cte AS
(SELECT *, ROW_NUMBER () OVER(
	PARTITION BY ParcelID,LandUse,PropertyAddress,SaleDate,SalePrice,LegalReference ORDER BY UniqueID)
    row_num1
    FROM housing.staging)
SELECT *
FROM duplicate_cte
WHERE row_num1 > 1;

ALTER TABLE staging
ADD COLUMN row_num int;

UPDATE staging t1
JOIN (
	SELECT UniqueID, ROW_NUMBER () OVER(
	PARTITION BY ParcelID,LandUse,SaleDate,SalePrice,LegalReference ORDER BY UniqueID) row_num
    FROM staging
    ) AS rn
	ON rn.UniqueID = t1.UniqueID
SET t1.row_num = rn.row_num;

DELETE
FROM staging
WHERE row_num > 1;

ALTER TABLE staging
DROP COLUMN row_num;

-- 2. Standardize the data

-- Change SaleDate to the proper date format.
SELECT SaleDate, STR_TO_DATE(`SaleDate`,"%M %e, %Y")
FROM staging;

UPDATE staging
SET SaleDate = STR_TO_DATE(`SaleDate`,"%M %e, %Y");

-- Populate PropertyAddress data.
-- Check for missing data. a PropertyAddress is tied to one ParcelID.
SELECT *
FROM staging
WHERE PropertyAddress IS NULL OR PropertyAddress = ''
-- ORDER BY UniqueID
ORDER BY ParcelID;

-- Check for corresponding data with the same ParcelID using self join.
SELECT t1.UniqueID, t1.ParcelID, t1.PropertyAddress, t2.UniqueID, t2.ParcelID, t2.PropertyAddress
FROM staging t1
JOIN staging t2
	ON t1.ParcelID = t2.ParcelID
    AND t1.UniqueID != t2.UniqueID
WHERE (t1.PropertyAddress IS NULL OR t1.PropertyAddress = '')
AND (t2.PropertyAddress IS NOT NULL AND t2.PropertyAddress != '');

-- Update staging table using self join.
UPDATE staging t1
JOIN staging t2
	ON t1.ParcelID = t2.ParcelID
    AND t1.UniqueID != t2.UniqueID
SET t1.PropertyAddress = t2.PropertyAddress
WHERE (t1.PropertyAddress IS NULL OR t1.PropertyAddress = '')
AND (t2.PropertyAddress IS NOT NULL AND t2.PropertyAddress != '');

-- Break PropertyAddress into Individual Columns (Address, City)

SELECT PropertyAddress,
SUBSTRING_INDEX(PropertyAddress,',',1) Address, TRIM(SUBSTRING_INDEX(PropertyAddress,',',-1)) City
FROM housing.staging;

ALTER TABLE housing.staging
ADD COLUMN (PropertySplitAddress text,
PropertySplitCity text);

UPDATE housing.staging
SET PropertySplitAddress = SUBSTRING_INDEX(PropertyAddress,',',1),
PropertySplitCity = TRIM(SUBSTRING_INDEX(PropertyAddress,',',-1));

-- Break OwnerAddress in Individual Columns (Address, City, State)

SELECT OwnerAddress,
	SUBSTRING_INDEX(OwnerAddress,',',1) Address,
	TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress,',',-2),',',1)) City,
    TRIM(SUBSTRING_INDEX(OwnerAddress,',',-1)) State
FROM housing.staging;

ALTER TABLE housing.staging
ADD COLUMN (OwnerSplitAddress text,
OwnerSplitCity text,
OwnerSplitState text);

UPDATE housing.staging
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress,',',1),
OwnerSplitCity = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress,',',-2),',',1)),
OwnerSplitState = TRIM(SUBSTRING_INDEX(OwnerAddress,',',-1));

-- Change different values that have similar meanings.
-- Change Y and N to Yes and No in SoldAsVacant.

SELECT SoldAsVacant, COUNT(SoldAsVacant)
FROM housing.staging
GROUP BY SoldAsVacant;

UPDATE housing.staging
SET SoldAsVacant = CASE
	WHEN SoldAsVacant = 'Y' THEN 'Yes'
    WHEN SoldAsVacant = 'N' THEN 'No'
    ELSE SoldAsVacant
END;

-- 3. Remove unnecessary columns or rows

SELECT *
FROM housing.staging;

ALTER TABLE housing.staging
DROP COLUMN PropertyAddress,
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict;
    

