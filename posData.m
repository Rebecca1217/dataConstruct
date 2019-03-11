
% �ֲֽṹ���ӣ���һ����Ա�ֲ־������仯�ʣ��������ӣ���������-����������
% CCommodityFuturesPositions(�й���Ʒ�ڻ��ɽ����ֲ�) ��Wind���ݿ�ֱ�Ӷ�ȡ


%% ��ȡ��Ա��λ�ֶ൥�����ֿյ�����������ԭʼ����

dateFrom = 20080101;
dateTo = 20190213;
tradingDay = gettradingday(dateFrom, dateTo);
tableData = getBasicData('future');
tableData.ContName = cellfun(@char, tableData.ContName, 'UniformOutput', false);

% codeName = getVarietyCode();
dataPos = array2table(nan(height(tradingDay), length(unique(tableData.ContName)) + 1));
dataPos.Properties.VariableNames = vertcat({'Date'}, unique(tableData.ContName))';
dataPos.Date = tradingDay.Date;


%% ��Wind���ݿ��ȡ����
% ������2018��3��29֮ǰ���������л�Ա��λ�ֲ֣�����Ҳֻ��ǰ20�ˣ����Բ��Կ϶�Ҫ���б��н������в�ͬ
% ֱ�Ӷ����ܺ�����ݰɣ�ȫ�������ܹ�2000������1��200�����ң���̫����MATLAB�����ˡ���sqlһ����Ҳ�����ˡ���
% ֻȡǰ20�����ܵĻ���������1/20��ֻ��103����

% Note��ȡ����ʱ��ȡ������ȡ�����Ժ��Լ����㣬ǰ20��ֱ����Ӽ�����������ǲ��Եģ�Ҳ����˵���ԣ���Ϊ���α仯��������������仯
% �������������������ݾ��ǵ���ǰ20���������ϼƣ�Ҳ�������������������������Ҫ�Լ�����
% ��20080103 ��C0805.DCEΪ����Wind���ݿ��ȡ��ǰ20���ʹ�������ǰչʾ��ǰ20�����ݲ�һ������19����һ����������
% �����ǽ������и���Windû�и��µ��µģ�Ӱ��Ӧ�ò���

conn = database('wind_fsync','query','query','com.microsoft.sqlserver.jdbc.SQLServerDriver',...
    'jdbc:sqlserver://10.201.4.164:1433;databaseName=wind_fsync');
sql = ['select S_Info_Windcode, TRADE_DT, FS_INFO_TYPE, sum(FS_INFO_POSITIONSNUM) as FS_Info_Positionssum, ', ...
    'from CCOMMODITYFUTURESPOSITIONS ', ...
    'where FS_INFO_RANK <= 20 and Trade_DT >=''', ...
    num2str(dateFrom),''' and Trade_DT <=''',num2str(dateTo),''' ', ...
    'group by S_INFO_WINDCODE, TRADE_DT, FS_INFO_TYPE order by TRADE_DT'];
cursorA = exec(conn,sql);
cursorB = fetch(cursorA);
res = cell2table(cursorB.Data, 'VariableNames', {'ContName', 'Date', 'Type', 'Value', 'DeltaValue'});
res.Date = str2double(res.Date); % ��ô�򵥵�һ�������������������������Ǹ����⡣��
% ���治��Ҫ��Wind���룬�Ѻ�׺ȥ����Ȼ���治��ƥ��
res.ContName = regexp(res.ContName, '\w+(?=\.)', 'match');
res.ContName = cellfun(@char, res.ContName, 'UniformOutput', false);

%% �ֽ���ֶ൥�����ֿյ������ɽ���3�����ݼ�
% res.Type 1 �ɽ��� 2 ������ 3 ��������
posLong = res(strcmp(res.Type, '2'), {'ContName', 'Date', 'Value'});
longVolume = tableData(:, {'Date', 'MainCont', 'ContName'});
longVolume = outerjoin(longVolume, posLong, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
longVolume = unstack(longVolume(:, {'Date', 'ContName_longVolume', 'Value'}), 'Value', 'ContName_longVolume');
% ���⣺�����ֵ�費��Ҫ�����Ѿ��������Ժ�Ŀ�ֵ��ʵ������0

posShort = res(strcmp(res.Type, '3'), {'ContName', 'Date', 'Value'});
shortVolume = tableData(:, {'Date', 'MainCont', 'ContName'});
shortVolume = outerjoin(shortVolume, posShort, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
shortVolume = unstack(shortVolume(:, {'Date', 'ContName_shortVolume', 'Value'}), 'Value', 'ContName_shortVolume');


trade = res(strcmp(res.Type, '1'), {'ContName', 'Date', 'Value'});
tradeVolume = tableData(:, {'Date', 'MainCont', 'ContName'});
tradeVolume = outerjoin(tradeVolume, trade, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
tradeVolume = unstack(tradeVolume(:, {'Date', 'ContName_tradeVolume', 'Value'}), 'Value', 'ContName_tradeVolume');

%% �Ȱ��յ�ǰ��ʽ�����������������Ĵ���ʽ���ܺ͵����Ӳ��Բ�̫һ��
save('E:\futureData\longVolume.mat', 'longVolume')
save('E:\futureData\shortVolume.mat', 'shortVolume')


%% ��Ҫ����ǰ20���仯�������ݣ��������Լ��������Ľ������
posLongDelta = res(strcmp(res.Type, '2'), {'ContName', 'Date', 'DeltaValue'});
longVolumeDelta = tableData(:, {'Date', 'MainCont', 'ContName'});
longVolumeDelta = outerjoin(longVolumeDelta, posLongDelta, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
longVolumeDelta = unstack(longVolumeDelta(:, {'Date', 'ContName_longVolumeDelta', 'DeltaValue'}), 'DeltaValue', 'ContName_longVolumeDelta');
% ���⣺�����ֵ�費��Ҫ�����Ѿ��������Ժ�Ŀ�ֵ��ʵ������0

posShortDelta = res(strcmp(res.Type, '3'), {'ContName', 'Date', 'DeltaValue'});
shortVolumeDelta = tableData(:, {'Date', 'MainCont', 'ContName'});
shortVolumeDelta = outerjoin(shortVolumeDelta, posShortDelta, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
shortVolumeDelta = unstack(shortVolumeDelta(:, {'Date', 'ContName_shortVolumeDelta', 'DeltaValue'}), 'DeltaValue', 'ContName_shortVolumeDelta');


tradeDelta = res(strcmp(res.Type, '1'), {'ContName', 'Date', 'DeltaValue'});
tradeVolumeDelta = tableData(:, {'Date', 'MainCont', 'ContName'});
tradeVolumeDelta = outerjoin(tradeVolumeDelta, tradeDelta, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
tradeVolumeDelta = unstack(tradeVolumeDelta(:, {'Date', 'ContName_tradeVolumeDelta', 'DeltaValue'}), 'DeltaValue', 'ContName_tradeVolumeDelta');

save('E:\futureData\longVolumeDelta.mat', 'longVolumeDelta')
save('E:\futureData\shortVolumeDelta.mat', 'shortVolumeDelta')

%% �������ݸ�ʽ
% dataPos = stack(dataPos, 2:width(dataPos), ...
%     'NewDataVariableName', 'SpotPrice', 'IndexVariableName', 'Variety');
% dataPos.Variety = arrayfun(@char, dataPos.Variety, 'UniformOutput', false);
% 
% dataPos = outerjoin(dataPos, spotCode(:, {'ContCode', 'ContName'}), 'type', 'left', 'MergeKeys', true,...
%     'LeftKeys', 'Variety', 'RightKeys', 'ContName');
% dataSpotNew = dataPos(:, {'Date', 'ContCode', 'SpotPrice'});
% dataSpotNew = sortrows(dataSpotNew, {'ContCode', 'Date'});
% save('E:\futureData\dataSpotNew.mat', 'dataSpotNew')






