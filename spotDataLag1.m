% w = windmatlab;
% [w_edb_data,w_edb_codes,w_edb_fields,w_edb_times,w_edb_errorid,w_edb_reqid]=w.edb('S5123087','2018-01-01','2019-02-13','Fill=Previous');
% [w_wsd_data,w_wsd_codes,w_wsd_fields,w_wsd_times,w_wsd_errorid,w_wsd_reqid]=w.wsd('W00017SPT.NM','close','2019-01-01','2019-02-12');

% ����ϸ�ڣ�
% 1���ֻ���ȡʱ��Ҫ�жϴ����Ǽ�����ͨ���жϴ�����char����cell������һ��Ҫ��Ȩ
% 2����ȡʱҪ�жϴ����Ƿ�Ϊ�գ�����ǿգ���ֱ�Ӳ��ö��˷���NaN���ɡ�
% 3������֮��Ҫ���Ե�λ����
% 4��ÿ��Ʒ�ֶ����Ժ�Ҫ��join��tradingDay�ϣ�fillmissing����stack
% 5��Ŀǰ�����ݣ��������Ҫ��Ȩ���ֻ���ÿ��code��������λ��һ���ģ���ʱ�����ǲ�ͬcode��λ��ͬ��Ҫ�������õ����

% @2019.03.07 �ֻ�����ȫ���ͺ�һ�죬���������̺��ȡ������ֻ�����

%% ��ȡ�ֻ����ݣ��̶�EDB���룩
% ȫ����EDB���룬û��W��ͷ������
load('para\spotCode.mat')
dateFrom = 20080101;
dateTo = 20190306;
tradingDay = gettradingday(dateFrom, dateTo);

dataSpot = array2table(nan(height(tradingDay), height(spotCode) + 1));
dataSpot.Properties.VariableNames = vertcat({'Date'}, spotCode.ContName)';
dataSpot.Date = tradingDay.Date; % ��������ǻز��Ӧ�������ݵ�����

% ���湹��һ��ʵ�ʶ�ȡ�ֻ������ڣ�������������ǰ��һ��������
readTradingDay = gettradingday(...
    str2double(datestr(datenum(num2str(dateFrom), 'yyyymmdd') - 15, 'yyyymmdd')), dateTo);
readBeginIdx = find(readTradingDay.Date == dataSpot.Date(1), 1) - 1;
readEndIdx = find(readTradingDay.Date == dataSpot.Date(end), 1) - 1;
readTradingDay = readTradingDay(readBeginIdx : readEndIdx, :);

% make sure readTradingDay and dataSpot.Date have the same size
if height(readTradingDay) ~= height(dataSpot)
    error('Check the readTradingDay dimension!')
end

w = windmatlab;

for iRow = 1:height(spotCode)
    spotCodeI = spotCode.SpotCode{iRow};
    % spotCodeI �����char������һ�����룬�����cell�����Ƕ�����룬�����double������NaN
    % ����ȫ��NaN��dataI����Ҫ���Ե�λ����fillmissing
    switch class(spotCodeI)
        case 'char'
            [w_edb_data,~,~,w_edb_times,w_edb_errorid,~]=...
                w.edb(spotCodeI, ...
                datestr(datenum(num2str(readTradingDay.Date(1)), 'yyyymmdd'), 'yyyy-mm-dd'),...
                datestr(datenum(num2str(readTradingDay.Date(end)), 'yyyymmdd'), 'yyyy-mm-dd'), ...
                'Fill=Previous');
            if w_edb_errorid ~= 0
                error('Wind Data Error!')
            end
            dataI = table(arrayfun(@(x) str2double(datestr(x, 'yyyymmdd')), w_edb_times), w_edb_data);
            dataI.Properties.VariableNames = {'Date', 'SpotDataI'};
            dataI = outerjoin(readTradingDay, dataI, 'type', 'left', 'MergeKeys', true);
            dataI.SpotDataI = dataI.SpotDataI * spotCode.SpotUnit(iRow);
            dataI.SpotDataI = fillmissing(dataI.SpotDataI, 'previous');
        case 'double'
            dataI = readTradingDay;
            dataI.SpotDataI = nan(height(readTradingDay), 1);
        case 'cell'
            dataI = readTradingDay;
            dataI(:, 2:length(spotCodeI) + 1) = array2table(nan(height(readTradingDay), length(spotCodeI)));
            for jCode = 1:length(spotCodeI)
                spotCodeIJ = spotCodeI{jCode};
                [w_edb_data,~,~,w_edb_times,w_edb_errorid,~]=...
                    w.edb(spotCodeIJ, ...
                    datestr(datenum(num2str(readTradingDay.Date(1)), 'yyyymmdd'), 'yyyy-mm-dd'),...
                    datestr(datenum(num2str(readTradingDay.Date(end)), 'yyyymmdd'), 'yyyy-mm-dd'), ...
                    'Fill=Previous');
                if w_edb_errorid ~= 0
                    error('Wind Data Error!')
                end
                dataIJ = table(arrayfun(@(x) str2double(datestr(x, 'yyyymmdd')), w_edb_times), w_edb_data);
                dataIJ.Properties.VariableNames = {'Date', 'SpotDataIJ'};
                dataIJ.SpotDataIJ = dataIJ.SpotDataIJ * spotCode.SpotWeight{iRow}(jCode);
                dataIJ = outerjoin(readTradingDay, dataIJ, 'type', 'left', 'MergeKeys', true);
                dataI(:, jCode + 1) = dataIJ(:, 2);
            end
            
            avgPrice = sum(table2array(dataI(:, 2:end)), 2); % ����omitnan�� �ӵ�һ�������Ŀ�ʼ
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
dataSpotNewLag1 = dataSpot(:, {'Date', 'ContCode', 'SpotPrice'});
dataSpotNewLag1 = sortrows(dataSpotNewLag1, {'ContCode', 'Date'});
save('E:\futureData\dataSpotNewLag1.mat', 'dataSpotNewLag1')






