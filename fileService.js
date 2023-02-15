//fileService.js
//Import variables
const productKeys = require('../config/OCC_netSuite_keys_map.json').product;
const variantKeys = require('../config/OCC_netSuite_keys_map.json').variant;
const colorObj = require('../config/product_variant_type.json').color;
const sizeObj = require('../config/product_variant_type.json').size;
const logger = require('../utils/globalLogger');
const configuration = require('../config');
const oicRestClient = require('../utils/oicRestClient');
const decode = require('unescape');
const csvtojson = require('csvtojson');
const admZip = require('adm-zip');
const path = require('path');
const fs = require('fs');
const feedService = require('./feedService');
const importService = require('./importService');
const assetPublishTools = require('../utils/assetPublishTools');

const moduleName = "FILESERVICE";

    module.exports = {

    //Function to convert CSV file to JSON and Import to OCC
    importFile: async (parentRequest) => {
        try {
            let productFileData = [];
            let variantFileData = [];
            let productModifiedFileData = [];
            let variantModifiedFileData = [];
            let imageFileData = [];
            let importData = [];
            let worksetId;
            let importMethod = parentRequest.hasOwnProperty('importProcess') ? parentRequest.importProcess : parentRequest;

            //convert  CSV file to JSON object
            for (let i = 0; i <= 4; i++) {
                if (i == 0) {
                    logger.debug(`${moduleName} | Converting products csv file to json`);
                    productFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicProductFileName}.csv`)); 
                } else if (i == 1) {
                    logger.debug(`${moduleName} | Converting variants csv file to json`);
                    variantFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicVariantFileName}.csv`));
                } else if (i == 2) {
                    logger.debug(`${moduleName} | Converting products modified csv file to json`);
                    productModifiedFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicProductModifiedFileName}.csv`));
                } else if (i == 3) {
                    logger.debug(`${moduleName} | Converting variants modified csv file to json`);
                    variantModifiedFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicVariantModifiedFileName}.csv`));
                } else if(importMethod === "product"){
                    logger.debug(`${moduleName} | Converting images csv file to json`);
                    imageFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicImagesFileName}.csv`));   
                }
            }
            module.exports.deleteFiles();
            let optimizedVariants = new Map();
            let optimizedImages = new Map();
            let presentColorsInVarients = new Map();
            let presentSizesInVarients = new Map();

            logger.debug(`${moduleName} | Processing Delta variants`);
            variantModifiedFileData.map(item => {
                if (item && item.hasOwnProperty('custitem_size_name_shop') && item.custitem_size_name_shop && !presentColorsInVarients.has(item.custitem_size_name_shop.toLowerCase())) {
                    presentSizesInVarients.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop.toLowerCase());
                }
                if (item && item.hasOwnProperty('custitem_color_name_shop') && item.custitem_color_name_shop && !presentSizesInVarients.has(item.custitem_color_name_shop.toLowerCase())) {
                    presentColorsInVarients.set(item.custitem_color_name_shop.toLowerCase(), item.custitem_color_name_shop.toLowerCase());
                }
                let productFound = productModifiedFileData.find(o => o.internalid === item.custom_internalid || o.internalid === item.internalid);
                productFound = !productFound ? productFileData.find(o => o.internalid === item.custom_internalid || o.internalid === item.internalid) : null;
                if(productFound){
                    productModifiedFileData.push(productFound);
                }
                if (item && !item.custom_internalid) {
                    optimizedVariants[`${item.internalid}_product`] = [];
                    optimizedVariants[`${item.internalid}_product`].push(item);
                } else {
                    if (optimizedVariants && optimizedVariants.hasOwnProperty(item.custom_internalid)) {
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    } else {
                        optimizedVariants[`${item.custom_internalid}`] = [];
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    }
                }
            });
            logger.debug(`${moduleName} | Processing Master variants`);
            variantFileData.map(item => {
                if (item && item.hasOwnProperty('custitem_size_name_shop') && item.custitem_size_name_shop && !presentColorsInVarients.has(item.custitem_size_name_shop.toLowerCase())) {
                    presentSizesInVarients.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop.toLowerCase());
                }
                if (item && item.hasOwnProperty('custitem_color_name_shop') && item.custitem_color_name_shop && !presentSizesInVarients.has(item.custitem_color_name_shop.toLowerCase())) {
                    presentColorsInVarients.set(item.custitem_color_name_shop.toLowerCase(), item.custitem_color_name_shop.toLowerCase());
                }

                let skuAvailableInDelta;
                let availableMapping = optimizedVariants[`${item.custom_internalid}`] ? optimizedVariants[`${item.custom_internalid}`] : optimizedVariants[`${item.internalid}_product`];
                if(availableMapping && availableMapping.length > 0){
                    skuAvailableInDelta = availableMapping.find(o => o.internalid === item.internalid);
                }

                if (item && !item.custom_internalid) {
                    if(!skuAvailableInDelta){
                        optimizedVariants[`${item.internalid}_product`] = [];
                        optimizedVariants[`${item.internalid}_product`].push(item);
                    }
                } else {
                    if (optimizedVariants && optimizedVariants.hasOwnProperty(item.custom_internalid)) {
                        if(!skuAvailableInDelta){
                            optimizedVariants[`${item.custom_internalid}`].push(item);
                        }
                    } else {
                        optimizedVariants[`${item.custom_internalid}`] = [];
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    }
                }
            });

            if(importMethod === "product"){
                logger.debug(`${moduleName} | Processing Images`);
                for(let i = 0; i < imageFileData.length; i++){
                    optimizedImages[`${imageFileData[i].formulatext}`] = [];
                    optimizedImages[`${imageFileData[i].formulatext}`].push(imageFileData[i]);
                }

                logger.debug(`${moduleName} | Updating product variants`);
                await updateProductTypeVariant([...presentSizesInVarients.values()], configuration.occ_config.sizeProductVaiantTypeUrl, sizeObj);
                await updateProductTypeVariant([...presentColorsInVarients.values()], configuration.occ_config.colorProductVaiantTypeUrl, colorObj);
                worksetId = await assetPublishTools.createWorkset('SSEWorkSet');
                logger.debug(`${moduleName} | Workset id is "${worksetId}"`);
            }

            logger.debug(`${moduleName} | Processing products.`);
            let productLength = 0;
            let skuLength = 0;
            let devideBy;
            let importResponse;
            if(importMethod === "product"){
                devideBy = configuration.batchOfProducts;
            }
            if(importMethod === "price"){
                devideBy = productModifiedFileData.length;
            }
            let productBatches =  Math.ceil(productModifiedFileData.length / devideBy);
            for(let i = 0; i <= productBatches - 1; i++) {
                let startFrom = devideBy * i;
                let endTo = devideBy + startFrom;
                logger.debug(`${moduleName} | startFrom ${startFrom} endTo ${endTo}`);
                let importFormatData = {product : []};
                let importPricetData = {price : []};
                for(let j=startFrom + 1; j <= endTo - 1; j++){
                    if (productModifiedFileData[j] && productModifiedFileData[j].custitem_product_price && productModifiedFileData[j].custitem_amz_item_name ) {
                        //filter the variant if sale pricve is greatr thatn list price and remove it from array
                        if(optimizedVariants[productModifiedFileData[j].internalid] && optimizedVariants[productModifiedFileData[j].internalid].length > 0){
                            for(let index = 0; index <= optimizedVariants[productModifiedFileData[j].internalid].length-1; index++){
                                let variant = optimizedVariants[productModifiedFileData[j].internalid][index];
                                if(!variant.custitem_shop_compare_price || (variant.custitem_occ_sale_price && variant.custitem_shop_compare_price && Number(variant.custitem_occ_sale_price) > Number(variant.custitem_shop_compare_price))){
                                    optimizedVariants[productModifiedFileData[j].internalid].splice(index,1); //remove variant from array
                                    index--;
                                }
                            }
                        }
                        //Condition to check variant/variants is/are available for product or not
                        if (productModifiedFileData[j].hasOwnProperty('custitem_amz_parent_child') && productModifiedFileData[j].custitem_amz_parent_child.toLowerCase() == 'standalone' && productModifiedFileData[j].hasOwnProperty('internalid')) {
                            productModifiedFileData[j].internalid = `${productModifiedFileData[j].internalid}_product`;
                        }
                        if(optimizedVariants[productModifiedFileData[j].internalid] && optimizedVariants[productModifiedFileData[j].internalid].length > 0){
                            if(importMethod === "price"){
                                productLength = productLength + 1; // To add log for products count 
                                let allSkus = optimizedVariants[productModifiedFileData[j].internalid];
                                for(let skuIndex = 0; skuIndex < allSkus.length; skuIndex++){
                                    skuLength = skuLength + 1; // To add log for skus count
                                    let priceBody = mapPrices(productModifiedFileData[j].internalid, allSkus[skuIndex]);
                                    if(priceBody && priceBody.length > 1){
                                        importPricetData.price.push(priceBody[0]);
                                        importPricetData.price.push(priceBody[1]);
                                    } else {
                                        importPricetData.price.push(priceBody[0]);
                                    }
                                }
                            } 
                            if(importMethod === "product") {
                                let productBody = mapKeys(productModifiedFileData[j], optimizedVariants, optimizedImages);
                                // Need to add seoMetaInfo
								let slugUrl = feedService.slugify(productBody.displayName);
								let seoMetaInfoData = {
									"seoUrlSlug" : slugUrl
								} 
								productBody["seoMetaInfo"] = seoMetaInfoData;
								importFormatData.product.push(productBody);
                                importData.push(productBody);
                            }
                        }
                    }
                    if(j === productFileData.length){
                        break;
                    }
                }
                const file = new admZip();
                if(importMethod === "product") {
                    logger.debug(`${moduleName} | Length of product array ${importFormatData.product.length}`);
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`), JSON.stringify(importFormatData));
                    configuration.contentJSON[0].fileName = `${configuration.importFileName}${i}.json`;
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`), JSON.stringify(configuration.contentJSON));
                    file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`));
                    file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`));
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.zip`), file.toBuffer());
                    fs.unlinkSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`));
                    await importService.uploadAndImportFileToOCC(`${configuration.importFileName}${i}`, worksetId);
                }
                if(importMethod === "price") {
                    logger.debug(`${moduleName} | Length of price array ${importPricetData.price.length}`);
                    logger.debug(`${moduleName} | Number of product ${productLength}`);
                    logger.debug(`${moduleName} | Number of variants  ${skuLength}`);
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importPriceFileName}${i}.json`), JSON.stringify(importPricetData));
                    configuration.priceContentJSON[0].fileName = `${configuration.importPriceFileName}${i}.json`;
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`), JSON.stringify(configuration.priceContentJSON));
                    file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.importPriceFileName}${i}.json`));
                    file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`));
                    fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importPriceFileName}${i}.zip`), file.toBuffer());
                    fs.unlinkSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importPriceFileName}${i}.json`));
                    importResponse = await importService.uploadAndImportFileToOCC(`${configuration.importPriceFileName}${i}`, '');
                }
            }

            if(importMethod === "product"){
                logger.debug(`${moduleName} | Processing data for Google feed file`);
                await feedService.processFeedData(importData, worksetId);
                module.exports.deleteFiles();
                logger.debug(`${moduleName} | Processing data for Google feed file is created sucesfully`);
                assetPublishTools.publishActiveWorkset(worksetId, configuration.occ_config.retryPublishInterval, 5);
            }
            if(importMethod === "price") {
                module.exports.deleteFiles();
                return parentRequest.callback(true, importResponse);
            }
        } catch (error) {
            logger.error(`${moduleName} |===============> Error : ${JSON.stringify(error)}`);
            logger.error(`${moduleName} |===============> Convert CSV file to JSON: Error : ${JSON.stringify(error.message)}`);
        }
    },

    uploadProductFile: async (parentRequest) => {
        try {
            let productFileData = [];
            let variantFileData = [];
            let imageFileData = [];
            let productModifiedFileData = [];
            let variantModifiedFileData = [];

            //convert  CSV file to JSON object
            logger.debug(`${moduleName} | Converting csv files to json`);
            for (let i = 0; i <= 4; i++) {
                if (i == 0) {
                    logger.debug(`${moduleName} | Converting products csv file to json`);
                    productFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicProductFileName}.csv`)); 
                } else if (i == 1) {
                    logger.debug(`${moduleName} | Converting variants csv file to json`);
                    variantFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicVariantFileName}.csv`));
                } else if (i == 2) {
                    logger.debug(`${moduleName} | Converting products modified csv file to json`);
                    productModifiedFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicProductModifiedFileName}.csv`));
                } else if (i == 3) {
                    logger.debug(`${moduleName} | Converting variants modified csv file to json`);
                    variantModifiedFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicVariantModifiedFileName}.csv`));
                } else {
                    logger.debug(`${moduleName} | Converting images csv file to json`);
                    imageFileData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', `${configuration.oicImagesFileName}.csv`));   
                }
            }
            module.exports.deleteFiles();
            let optimizedVariants = new Map();
            let optimizedImages = new Map();
            let presentColorsInVarients = new Map();
            let presentSizesInVarients = new Map();
            let duplicateColorEntries= new Map();
            let duplicateSizeEntries= new Map();
            
            logger.debug(`${moduleName} | Processing Delta variants`);
            variantModifiedFileData.map(item => {
                let productFound = productModifiedFileData.find(o => o.internalid === item.custom_internalid || o.internalid === item.internalid);
                productFound = !productFound ? productFileData.find(o => o.internalid === item.custom_internalid || o.internalid === item.internalid) : null;
                if(productFound){
                    productModifiedFileData.push(productFound);
                }
                if (item && !item.custom_internalid) {
                    optimizedVariants[`${item.internalid}_product`] = [];
                    optimizedVariants[`${item.internalid}_product`].push(item);
                } else {
                    if (optimizedVariants && optimizedVariants.hasOwnProperty(item.custom_internalid)) {
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    } else {
                        optimizedVariants[`${item.custom_internalid}`] = [];
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    }
                }
              if (item && item.hasOwnProperty('custitem_size_name_shop') && item.custitem_size_name_shop && !presentColorsInVarients.has(item.custitem_size_name_shop)) {
                    if(presentSizesInVarients && !presentSizesInVarients.has(item.custitem_size_name_shop.toLowerCase())){
                    presentSizesInVarients.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop);
                    }else{
                        duplicateSizeEntries.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop);
                    }
                }
               if (item && item.hasOwnProperty('custitem_color_name_shop') && item.custitem_color_name_shop && !presentSizesInVarients.has(item.custitem_color_name_shop)) {
                    if(presentColorsInVarients && !presentColorsInVarients.has(item.custitem_color_name_shop.toLowerCase())){
                    presentColorsInVarients.set(item.custitem_color_name_shop.toLowerCase(), item.custitem_color_name_shop);
                 } else{
                    duplicateColorEntries.set(item.custitem_size_name_shop.toLowerCase(),item.custitem_size_name_shop)
                 } 
               }
            });
            logger.debug(`${moduleName} | Processing Master variants`);
            variantFileData.map(item => {
                if (item && item.hasOwnProperty('custitem_size_name_shop') && item.custitem_size_name_shop && !presentColorsInVarients.has(item.custitem_size_name_shop)) {
                    if(presentSizesInVarients && !presentSizesInVarients.has(item.custitem_size_name_shop.toLowerCase())){
                    presentSizesInVarients.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop);
                    }else{
                        duplicateSizeEntries.set(item.custitem_size_name_shop.toLowerCase(), item.custitem_size_name_shop);
                    }
                }
                if (item && item.hasOwnProperty('custitem_color_name_shop') && item.custitem_color_name_shop && !presentSizesInVarients.has(item.custitem_color_name_shop)) {                    
                 if(presentColorsInVarients && !presentColorsInVarients.has(item.custitem_color_name_shop.toLowerCase())){
                    presentColorsInVarients.set(item.custitem_color_name_shop.toLowerCase(), item.custitem_color_name_shop);
                 }else{
                    duplicateColorEntries.set(item.custitem_size_name_shop.toLowerCase(),item.custitem_size_name_shop)
                 } 
                }
                          
                  
                let skuAvailableInDelta;
                let availableMapping = optimizedVariants[`${item.custom_internalid}`] ? optimizedVariants[`${item.custom_internalid}`] : optimizedVariants[`${item.internalid}_product`];
                if(availableMapping && availableMapping.length > 0){
                    skuAvailableInDelta = availableMapping.find(o => o.internalid === item.internalid);
                }

                if (item && !item.custom_internalid) {
                    if(!skuAvailableInDelta){
                        optimizedVariants[`${item.internalid}_product`] = [];
                        optimizedVariants[`${item.internalid}_product`].push(item);
                    }
                } else {
                    if (optimizedVariants && optimizedVariants.hasOwnProperty(item.custom_internalid)) {
                        if(!skuAvailableInDelta){
                            optimizedVariants[`${item.custom_internalid}`].push(item);
                        }
                    } else {
                        optimizedVariants[`${item.custom_internalid}`] = [];
                        optimizedVariants[`${item.custom_internalid}`].push(item);
                    }
                }
            });              
                  let sizeSet = new Set(duplicateSizeEntries);
                  let colorSet = new Set(duplicateColorEntries);
                 logger.debug("START --- Of duplicate SIZE in File");
                 logger.debug(JSON.stringify([...sizeSet]));
                 logger.debug("END --- Of duplicate SIZE in File");
                 logger.debug("START --- Of duplicate COLOR  in File");
                 logger.debug(JSON.stringify([...colorSet])); 
                 logger.debug("END  --- Of duplicate COLOR  in File");

            logger.debug(`${moduleName} | Processing Images`);
            for (let i = 0; i < imageFileData.length; i++) {
                optimizedImages[`${imageFileData[i].formulatext}`] = [];
                optimizedImages[`${imageFileData[i].formulatext}`].push(imageFileData[i]);
            }

            logger.debug(`${moduleName} | Updating product variants`);
           let finalVariantValues= await compareVariantsValues([...presentSizesInVarients.values()],[...presentColorsInVarients.values()]);
            await updateProductTypeVariant(finalVariantValues.success.x_sizeSet, configuration.occ_config.sizeProductVaiantTypeUrl, sizeObj);
            await updateProductTypeVariant(finalVariantValues.success.x_colorSet, configuration.occ_config.colorProductVaiantTypeUrl, colorObj);
            //Create workset id
            let worksetId = await assetPublishTools.createWorkset('SSEWorkSet');
            logger.debug(`${moduleName} | Workset id is "${worksetId}"`);

            let devideBy = configuration.batchOfProducts;
            let productBatches =  Math.ceil(productModifiedFileData.length / devideBy);

            logger.debug(`${moduleName} | Processing products.`);
            uploadFiles(productBatches, devideBy, productModifiedFileData, optimizedVariants, optimizedImages, worksetId, productFileData);
            let fileNames = [];
            for (let i = 0; i <= productBatches - 1; i++) {
                let fileNameObj = {
                    "name": `${configuration.importFileName}${i}.zip`
                }
                fileNames.push(fileNameObj);
            }
            let finalResponse = {
                "worksetId": worksetId,
                "files": fileNames
            }
            return parentRequest.callback(true, finalResponse);
        } catch (error) {
            logger.error(`${moduleName} |===============> Error : ${JSON.stringify(error)}`);
            logger.error(`${moduleName} |===============> Convert CSV file to JSON: Error : ${JSON.stringify(error.message)}`);
            return parentRequest.callback(false, error.message);
        }
    },

    inactiveFeedCall: async (inactiveReq) => {
        let url = configuration.getFileOIC + 'inactive';
        let getFile = await oicRestClient.getFileFromOIC(url, 'inactive.csv');
        if (getFile.error) {
            logger.error(`${moduleName} |===============> Get file from OIC: Error : ${JSON.stringify(getFile.errorMessage)}`);
            return inactiveReq.callback(false, getFile.errorMessage);
        }
        let csvtojsonData = await csvtojson().fromFile(path.resolve(__dirname, '..', 'inputs', "inactive.csv"));
        let productArray = [];
        let skuArray = [];
        csvtojsonData.map((item) => {
            if (item && item.custitem_amz_parent_child === '1') {
                skuArray.push(item);
            }
            if (item && item.custitem_amz_parent_child === '2') {
                productArray.push(item);
            }
        })
        let deativateProduct = await productDeactivationCall(productArray);
        let deativateSku = await skuDeactivationCall(skuArray);

        logger.info("executed Data = " + JSON.stringify({ deativateProduct, deativateSku }));
        return inactiveReq.callback(true, { deativateProduct, deativateSku });
    },

    deleteFiles: () => {
        let directory = path.resolve(__dirname,"..",'inputs');
        fs.readdir(directory, (err, files) => {
            if (err) throw err;
            for (const file of files) {
                if(file !== 'empty.txt' && file !== "promos.csv"){
                    fs.unlink(path.join(directory, file), error => {
                        if (err) throw error;
                    });            
                }
            }
          });
        return true;
    } 
}

/** get the entire product data and SKUs */
let getMasterProductData = (optimizedVariants, optimizedImages, masterproductFileData) => {
    try {
        logger.debug(`${moduleName} | starting to Constructing the entire master data`);
        let importData = [];
            for (let j = 0; j <= masterproductFileData.length; j++) {
                if (masterproductFileData[j] && masterproductFileData[j].custitem_product_price && masterproductFileData[j].custitem_amz_item_name) {
                    //filter the variant if sale pricve is greatr thatn list price and remove it from array
                    if (optimizedVariants[masterproductFileData[j].internalid] && optimizedVariants[masterproductFileData[j].internalid].length > 0) {
                        for (let index = 0; index <= optimizedVariants[masterproductFileData[j].internalid].length - 1; index++) {
                            let variant = optimizedVariants[masterproductFileData[j].internalid][index];
                            if (!variant.custitem_shop_compare_price || (variant.custitem_occ_sale_price && variant.custitem_shop_compare_price && Number(variant.custitem_occ_sale_price) > Number(variant.custitem_shop_compare_price))) {
                                optimizedVariants[masterproductFileData[j].internalid].splice(index, 1); //remove variant from array
                                index--;
                            }
                        }
                    }
                    if (masterproductFileData[j].hasOwnProperty('custitem_amz_parent_child') && masterproductFileData[j].custitem_amz_parent_child.toLowerCase() == 'standalone' && masterproductFileData[j].hasOwnProperty('internalid')) {
                        masterproductFileData[j].internalid = `${masterproductFileData[j].internalid}_product`;
                    }
                    //Condition to check variant/variants is/are available for product or not
                    if (optimizedVariants[masterproductFileData[j].internalid] && optimizedVariants[masterproductFileData[j].internalid].length > 0) {
                        let productBody = mapKeys(masterproductFileData[j], optimizedVariants, optimizedImages);
                        // Need to add seoMetaInfo
						let slugUrl = feedService.slugify(productBody.displayName);
                        let seoMetaInfoData = {
                            "seoUrlSlug" : slugUrl,
                            "seoTitle" : productBody.displayName
                        } 
                        productBody["seoMetaInfo"] = seoMetaInfoData;
                    
                        importData.push(productBody);
                    }
                }
            }
            logger.debug(`${moduleName} | Length of product array ${importData.length}`);
            return importData;
    } catch(err) {
        logger.debug(` Error Happened Constructing the master records for Google XML Feed`);
        return [];
    }
}

let uploadFiles = async (productBatches, devideBy, productFileData, optimizedVariants, optimizedImages, worksetId, masterproductFileData) => {
    try {
        logger.debug(`${moduleName} | starting to upload and import.`);
        let importData = [];
        let sizeSortProducts=[];
        for (let i = 0; i <= productBatches - 1; i++) {
            let startFrom = devideBy * i;
            let endTo = devideBy + startFrom;
            logger.debug(`${moduleName} | startFrom ${startFrom} endTo ${endTo}`);
            let importFormatData = { product: [] };
            for (let j = startFrom + 1; j <= endTo - 1; j++) {
                if (productFileData[j] && productFileData[j].custitem_product_price && productFileData[j].custitem_amz_item_name) {
                    //filter the variant if sale pricve is greatr thatn list price and remove it from array
                    if (optimizedVariants[productFileData[j].internalid] && optimizedVariants[productFileData[j].internalid].length > 0) {
                        for (let index = 0; index <= optimizedVariants[productFileData[j].internalid].length - 1; index++) {
                            let variant = optimizedVariants[productFileData[j].internalid][index];
                            if (!variant.custitem_shop_compare_price || (variant.custitem_occ_sale_price && variant.custitem_shop_compare_price && Number(variant.custitem_occ_sale_price) > Number(variant.custitem_shop_compare_price))) {
                                optimizedVariants[productFileData[j].internalid].splice(index, 1); //remove variant from array
                                index--;
                            }
                        }
                    }
                    if (productFileData[j].hasOwnProperty('custitem_amz_parent_child') && productFileData[j].custitem_amz_parent_child.toLowerCase() == 'standalone' && productFileData[j].hasOwnProperty('internalid')) {
                        productFileData[j].internalid = `${productFileData[j].internalid}_product`;
                    }
                    //Condition to check variant/variants is/are available for product or not
                    if (optimizedVariants[productFileData[j].internalid] && optimizedVariants[productFileData[j].internalid].length > 0) {
                        let productBody = mapKeys(productFileData[j], optimizedVariants, optimizedImages);
                        // Need to add seoMetaInfo
						let slugUrl = feedService.slugify(productBody.displayName);
                        let seoMetaInfoData = {
                            "seoUrlSlug" : slugUrl,
                            "seoTitle" : productBody.displayName
                        } 
                        productBody["seoMetaInfo"] = seoMetaInfoData;
                        importFormatData.product.push(productBody);
                        importData.push(productBody);
                        sizeSortProducts.push(productBody.id);
                    }
                }
                if (j === productFileData.length) {
                    break;
                }
            }
            logger.debug(`${moduleName} | Length of product array ${importFormatData.product.length}`);
            fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`), JSON.stringify(importFormatData));
            configuration.contentJSON[0].fileName = `${configuration.importFileName}${i}.json`;
            fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`), JSON.stringify(configuration.contentJSON));

            const file = new admZip();
            file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`));
            file.addLocalFile(path.resolve(__dirname, '..', 'inputs', `${configuration.contentFileName}.json`));
            fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.zip`), file.toBuffer());
            fs.unlinkSync(path.resolve(__dirname, '..', 'inputs', `${configuration.importFileName}${i}.json`));

            await importService.uploadFileToOCC(`${configuration.importFileName}${i}`);
        }
        module.exports.deleteFiles();
        logger.debug(`${moduleName} | Processing data for Google feed file`);
        let masterImportData = getMasterProductData(optimizedVariants, optimizedImages, masterproductFileData);
        await feedService.processFeedData(masterImportData, worksetId);
        logger.debug(`${moduleName} | Processing data for Google feed file is created sucesfully`);       
        fs.writeFileSync(path.resolve(__dirname, '..', 'inputs', 'sizeSortProducts.json'), JSON.stringify(sizeSortProducts));
        await feedService.processSizeSortProducts(worksetId);
        logger.debug("List of Products to be Sort" + JSON.stringify(sizeSortProducts));
        module.exports.deleteFiles();
        return 'Success';
    } catch (error) {
        logger.error(`${moduleName} |===============> Error : ${JSON.stringify(error)}`);
        logger.error(`${moduleName} |===============> Export uploadFiles file request : Error : ${JSON.stringify(error.message)}`);
        return { error: true, success: error.message };
    }
}

//Create object by mapping Netsuits object key to OCC object keys
let mapKeys = (row, optimizedVariants, optimizedImages) => {
    let productKey = Object.keys(productKeys);
    let newObj = Object.assign({}, productKeys);
    for (let item of productKey) {
        let emptyValue = null;
        let swatchMap = new Map();
        if (item === 'parentCategories') {
            let catArray = [];
            let categoryIds = row[`${newObj[item]}`] ? row[`${newObj[item]}`].split("|") : [];
            categoryIds.forEach(catId => {
                let parentCategoriesAttr = {
                    id: ""
                };
                parentCategoriesAttr.id = catId;
                catArray.push(parentCategoriesAttr);
            })
            newObj[item] = catArray;
        }
        if (item === 'longDescription') {
            let htmlText = row[`${newObj[item]}`];
            newObj[item] = row[`${newObj[item]}`] ? decode(htmlText) : null;
        }
        if (item === 'productImages') {
            newObj[item] = [];
            let productImageData = row[`itemid`] && optimizedImages[row[`itemid`]] ? optimizedImages[row[`itemid`]][0] : null;
            let imgURLs = mapImageUrls(productImageData);
            if (imgURLs.length > 0) {
                for (let imageData of imgURLs) {
                    newObj[item].push({
                        path: `/products/${imageData}`,
                        name: imageData,
                        metadata: {},
                        tags: []
                    })
                }
            }
        }
        if (item === 'x_gender') {
            let genders = row[`${newObj[item]}`] ? row[`${newObj[item]}`].split(",") : [];
            if(genders.indexOf('Womens') > -1 && genders.indexOf('Mens') > -1){
                newObj[item] = 'Unisex';
            }else if(genders.indexOf('Boys') > -1 && genders.indexOf('Girls') > -1){
                newObj[item] = 'Unisex';
            } else if(genders.indexOf('Boys') > -1 || genders.indexOf('Mens') > -1){
                newObj[item] = 'Men';
            } else if(genders.indexOf('Womens') > -1 || genders.indexOf('Girls') > -1){
                newObj[item] = 'Women';
            } else {
                newObj[item] = null;
            }
        }
        if (newObj[`${item}`] && typeof (newObj[`${item}`]) === "string" && item !== 'type' && item !== 'longDescription' && item !== 'x_gender') {
            let value = row.hasOwnProperty(`${newObj[item]}`) && row[`${newObj[item]}`] ? row[`${newObj[item]}`] : emptyValue;
            newObj[item] = value;
        }
        if (item === "childSKUs") {
            let variantKey = Object.keys(variantKeys);
            let sku = optimizedVariants[row['internalid']];
            if (sku && sku.length > 0) {
				let availableSizes = [], avaiableColors = [], variantValuesOrder={}, childSkusArray = [];
                newObj[item] = [];
                for (let skuItem of sku) {
                    let newVarObj = Object.assign({}, variantKeys);
                    let swatchImageName = '';
                    for (let keyItem of variantKey) {
                        if (keyItem === 'images') {
                            newVarObj[keyItem] = [];
                            let skuImageData = skuItem[`${newVarObj.x_netSuiteItemId}`] && optimizedImages[skuItem[`${newVarObj.x_netSuiteItemId}`]] ? optimizedImages[skuItem[`${newVarObj.x_netSuiteItemId}`]][0] : null;
                            let imgURLs = mapImageUrls(skuImageData);
                            if (imgURLs.length > 0) {
                                for (let imageData of imgURLs) {
                                    newVarObj[keyItem].push({
                                        path: `/products/${imageData}`,
                                        name: imageData,
                                        metadata: {},
                                        tags: []
                                    })
                                }
                            }
                            swatchImageName = imgURLs[0];
                        }
                        if(keyItem === 'listPrices'){
                            let price = {
                                "defaultPriceGroup": null
                              }
                            price.defaultPriceGroup = skuItem[`${newVarObj[keyItem]}`] ? skuItem[`${newVarObj[keyItem]}`] : emptyValue;
                            newVarObj[keyItem] = price;
                        }
                        if(keyItem === 'salePrices'){
                            let price = {
                                "defaultPriceGroup": null
                              }
                            price.defaultPriceGroup = skuItem[`${newVarObj[keyItem]}`] ? skuItem[`${newVarObj[keyItem]}`] : emptyValue;
                            newVarObj[keyItem] = price;
                        }
                        if (keyItem === 'translations') {
                            let translationAttr = {
                                items: [
                                    {
                                        displayName: "",
                                        parentSkus: [],
                                        lang: "en"
                                    }
                                ]
                            };
                            translationAttr.items[0].displayName = skuItem[`${newVarObj[keyItem]}`] ? skuItem[`${newVarObj[keyItem]}`] : emptyValue;
                            newVarObj[keyItem] = translationAttr;
                        }
                        if (newVarObj[`${keyItem}`] && typeof (newVarObj[`${keyItem}`]) === "string" && keyItem !== 'type') {
                            newVarObj[keyItem] = skuItem.hasOwnProperty(`${newVarObj[keyItem]}`) && skuItem[`${newVarObj[keyItem]}`] ? skuItem[`${newVarObj[keyItem]}`] : emptyValue;
                        }
                        if(keyItem === 'x_color' || keyItem === 'x_size'){
                            newVarObj[keyItem] = newVarObj[keyItem] ? newVarObj[keyItem] : newVarObj[keyItem];
                            if (keyItem === 'x_size' && newVarObj[keyItem] && !availableSizes.includes(newVarObj[keyItem])) {
                                availableSizes.push(newVarObj[keyItem]);
                            }
                            if (keyItem === 'x_color' && newVarObj[keyItem] && !avaiableColors.includes(newVarObj[keyItem])) {
                                avaiableColors.push(newVarObj[keyItem]);
                            }
						}
                        if (keyItem === 'x_color' && row['custitem_amz_parent_child'] === 'standalone' && skuItem['custitem_color_name_shop'] === '' && skuItem['custitem_size_name_shop'] === '') {
                            newVarObj[keyItem] = 'no_color';
                        }
                        let color = skuItem[`${newVarObj['x_color']}`];
                        if (color && swatchImageName) {
                            swatchMap.set(color.toLowerCase(), swatchImageName);
                        }
                    }
                    childSkusArray.push(newVarObj);
                }
                let obj = Array.from(swatchMap).reduce((swatchObj, [key, value]) => (
                    Object.assign(swatchObj, { [key]: value }) // Be careful! Maps can have non-String keys; object literals can't.
                ), {});
                newObj['x_swatchMapping'] = JSON.stringify(obj);
                newObj[item] = childSkusArray;
                if(availableSizes.length > 0 ){
                    variantValuesOrder['x_size'] =availableSizes;
                }
                if(avaiableColors.length > 0 ){
                    variantValuesOrder ['x_color'] =avaiableColors;
                } 
                newObj['variantValuesOrder'] = variantValuesOrder;
            }
        }
    }
    return newObj;
}
