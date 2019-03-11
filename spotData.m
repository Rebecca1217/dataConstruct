% w = windmatlab;
% [w_edb_data,w_edb_codes,w_edb_fields,w_edb_times,w_edb_errorid,w_edb_reqid]=w.edb('S5123087','2018-01-01','2019-02-13','Fill=Previous');
% [w_wsd_data,w_wsd_codes,w_wsd_fields,w_wsd_times,w_wsd_errorid,w_wsd_reqid]=w.wsd('W00017SPT.NM','close','2019-01-01','2019-02-12');

% 几个细节：
% 1、现货读取时，要判断代码是几个，通过判断代码是char还是cell，超过一个要加权
% 2、读取时要判断代码是否为空，如果是空，就直接不用读了返回NaN即可。
% 3、读完之后要乘以单位乘数
% 4、每个品种读完以后要先join到tradingDay上，fillmissing后再stack
% 5、目前的数据，如果是需要加权的现货，每个code读的数单位是一样的，暂时不考虑不同code单位不同需要单独设置的情况

%% 读取现货数据（固定EDB代码）
% 全都是EDB代码，没有W开头的那种
load('para\spotCode.mat')
dateFrom = 20080101;
dateTo = 20190306;
tradingDay = gettradingday(dateFrom, dateTo);

dataSpot = array2table(nan(height(tradingDay), height(spotCode) + 1));
dataSpot.Properties.VariableNames = vertcat({'Date'}, spotCode.ContName)';
dataSpot.Date = tradingDay.Date;

w = windmatlab;

for iRow = 1:height(spotCode)
    spotCodeI = spotCode.SpotCode{iRow};
    % spotCodeI 如果是char，就是一个代码，如果是cell，则是多个代码，如果是double，则是NaN
    % 除了全是NaN的dataI都需要乘以单位，并fillmissing
    switch class(spotCodeI)
        case 'char'
            [w_edb_data,~,~,w_edb_times,w_edb_errorid,~]=...
                w.edb(spotCodeI, ...
                datestr(datenum(num2str(dateFrom), 'yyyymmdd'), 'yyyy-mm-dd'),...
                datestr(datenum(num2str(dateTo), 'yyyymmdd'), 'yyyy-mm-dd'), ...
                'Fill=Previous');
            if w_edb_errorid ~= 0
                error('Wind Data Error!')
            end
            dataI = table(arrayfun(@(x) str2double(datestr(x, 'yyyymmdd')), w_edb_times), w_edb_data);
            dataI.Properties.VariableNames = {'Date', 'SpotDataI'};
            dataI = outerjoin(tradingDay, dataI, 'type', 'left', 'MergeKeys', true);
            dataI.SpotDataI = dataI.SpotDataI * spotCode.SpotUnit(iRow);
            dataI.SpotDataI = fillmissing(dataI.SpotDataI, 'previous');
        case 'double'
            dataI = tradingDay;
            dataI.SpotDataI = nan(height(tradingDay), 1);
        case 'cell'
            dataI = tradingDay;
            dataI(:, 2:length(spotCodeI) + 1) = array2table(nan(height(tradingDay), length(spotCodeI)));
            for jCode = 1:length(spotCodeI)
                spotCodeIJ = spotCodeI{jCode};
                [w_edb_data,~,~,w_edb_times,w_edb_errorid,~]=...
                    w.edb(spotCodeIJ, ...
                    datestr(datenum(num2str(dateFrom), 'yyyymmdd'), 'yyyy-mm-dd'),...
                    datestr(datenum(num2str(dateTo), 'yyyymmdd'), 'yyyy-mm-dd'), ...
                    'Fill=Previous');
                if w_edb_errorid ~= 0
                    error('Wind Data Error!')
                end
                dataIJ = table(arrayfun(@(x) str2double(datestr(x, 'yyyymmdd')), w_edb_times), w_edb_data);
                dataIJ.Properties.VariableNames = {'Date', 'SpotDataIJ'};
                dataIJ.SpotDataIJ = dataIJ.SpotDataIJ * spotCode.SpotWeight{iRow}(jCode);
                dataIJ = outerjoin(tradingDay, dataIJ, 'type', 'left', 'MergeKeys', true);
                dataI(:, jCode + 1) = dataIJ(:, 2);
            end
            
            avgPrice = sum(table2array(dataI(:, 2:end)), 2); % 不能omitnan， 从第一个有数的开始
            dataI(:, 2:end) = [];
            dataI.SpotDataI = avgPrice;
            dataI.SpotDataI = dataI.SpotDataI * spotCode.SpotUnit(iRow);
            dataI.SpotDataI = fillmissing(dataI.SpotDataI, 'previous');
    end
    
    dataSpot(:, iRow + 1) = dataI(:, 2);
end


dataSpot = stack(dataSpot, 2:width(dataSpot), ...
    'NewDataVariableName', 'SpotPrice', 'IndexVariableName', 'Variety');
dataSpot.Variety = arrayfun(@char, dataSpot.Variety, 'UniformOutput', false);

dataSpot = outerjoin(dataSpot, spotCode(:, {'ContCode', 'ContName'}), 'type', 'left', 'MergeKeys', true,...
    'LeftKeys', 'Variety', 'RightKeys', 'ContName');
dataSpotNew = dataSpot(:, {'Date', 'ContCode', 'SpotPrice'});
dataSpotNew = sortrows(dataSpotNew, {'ContCode', 'Date'});
save('E:\futureData\dataSpotNew.mat', 'dataSpotNew')






