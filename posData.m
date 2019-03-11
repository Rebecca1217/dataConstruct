
% 持仓结构因子：第一名会员持仓净买单量变化率（正向因子）（持买单量-持卖单量）
% CCommodityFuturesPositions(中国商品期货成交及持仓) 从Wind数据库直接读取


%% 读取会员单位持多单量、持空单量、交易量原始数据

dateFrom = 20080101;
dateTo = 20190213;
tradingDay = gettradingday(dateFrom, dateTo);
tableData = getBasicData('future');
tableData.ContName = cellfun(@char, tableData.ContName, 'UniformOutput', false);

% codeName = getVarietyCode();
dataPos = array2table(nan(height(tradingDay), length(unique(tableData.ContName)) + 1));
dataPos.Properties.VariableNames = vertcat({'Date'}, unique(tableData.ContName))';
dataPos.Date = tradingDay.Date;


%% 从Wind数据库读取数据
% 大商所2018年3月29之前公布过所有会员单位持仓，现在也只有前20了，所以策略肯定要跟研报中讲的略有不同
% 直接读汇总后的数据吧，全读出来总共2000万条（1年200万左右）量太大了MATLAB处理不了。。sql一次性也读不了。。
% 只取前20名汇总的话缩减超过1/20，只有103万行

% Note：取数的时候不取增减，取出来以后自己计算，前20名直接相加计算的增减量是不对的，也不能说不对，因为名次变化，结果不是总量变化
% 交易所公布的增减数据就是当天前20名的增减合计，也不是总量增减，所以这个变量要自己计算
% 以20080103 、C0805.DCE为例，Wind数据库读取的前20名和大商所当前展示的前20名数据不一样，第19名多一个吉粮集团
% 可能是交易所有更新Wind没有更新导致的，影响应该不大。

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
res.Date = str2double(res.Date); % 这么简单的一步操作都很慢，数据量级真是个问题。。
% 下面不需要用Wind代码，把后缀去掉不然后面不好匹配
res.ContName = regexp(res.ContName, '\w+(?=\.)', 'match');
res.ContName = cellfun(@char, res.ContName, 'UniformOutput', false);

%% 分解出持多单量、持空单量、成交量3个数据集
% res.Type 1 成交量 2 持买单量 3 持卖单量
posLong = res(strcmp(res.Type, '2'), {'ContName', 'Date', 'Value'});
longVolume = tableData(:, {'Date', 'MainCont', 'ContName'});
longVolume = outerjoin(longVolume, posLong, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
longVolume = unstack(longVolume(:, {'Date', 'ContName_longVolume', 'Value'}), 'Value', 'ContName_longVolume');
% 问题：这里空值需不需要处理？已经有数据以后的空值其实都该是0

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

%% 先按照当前格式保存下来，接下来的处理方式可能和单因子策略不太一样
save('E:\futureData\longVolume.mat', 'longVolume')
save('E:\futureData\shortVolume.mat', 'shortVolume')


%% 需要构造前20名变化量的数据，用总量自己相减计算的结果不好
posLongDelta = res(strcmp(res.Type, '2'), {'ContName', 'Date', 'DeltaValue'});
longVolumeDelta = tableData(:, {'Date', 'MainCont', 'ContName'});
longVolumeDelta = outerjoin(longVolumeDelta, posLongDelta, 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', {'Date', 'MainCont'}, 'RightKeys', {'Date', 'ContName'});
longVolumeDelta = unstack(longVolumeDelta(:, {'Date', 'ContName_longVolumeDelta', 'DeltaValue'}), 'DeltaValue', 'ContName_longVolumeDelta');
% 问题：这里空值需不需要处理？已经有数据以后的空值其实都该是0

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

%% 调整数据格式
% dataPos = stack(dataPos, 2:width(dataPos), ...
%     'NewDataVariableName', 'SpotPrice', 'IndexVariableName', 'Variety');
% dataPos.Variety = arrayfun(@char, dataPos.Variety, 'UniformOutput', false);
% 
% dataPos = outerjoin(dataPos, spotCode(:, {'ContCode', 'ContName'}), 'type', 'left', 'MergeKeys', true,...
%     'LeftKeys', 'Variety', 'RightKeys', 'ContName');
% dataSpotNew = dataPos(:, {'Date', 'ContCode', 'SpotPrice'});
% dataSpotNew = sortrows(dataSpotNew, {'ContCode', 'Date'});
% save('E:\futureData\dataSpotNew.mat', 'dataSpotNew')






