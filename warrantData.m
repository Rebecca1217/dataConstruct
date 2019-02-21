% warrantData  注册仓单数据
% [w_wss_data,w_wss_codes,w_wss_fields,w_wss_times,w_wss_errorid,w_wss_reqid]=w.wss('CU1906.SHF,CU1907.SHF,CU1908.SHF,CU1909.SHF','st_stock','tradeDate=20190217')
addpath dataDealingFunc
%% 几个细节
% 1、注册仓单数量只选择主力合约，需要先准备每个品种的当前主力合约代码；来源：漫雪TableData
% 2、用WSS读取每个合约每天的数据，需要循环日期，读到20190105需要20分钟；注册仓单是按品种区分，同一天同品种的所有合约注册仓单数一样

% 3、郑商所，每个品种Wind代码都少一位。。

%% 数据准备：
contPath = 'Z:\baseData\TableData\futureData\TableData.mat';
load(contPath);
dataWarrant = TableData(:, {'date', 'code', 'mainCont'});
load('para\codeName.mat')

w = windmatlab;
dataWarrant = outerjoin(dataWarrant, codeName(:, {'ContCode', 'Suffix'}), 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', 'code', 'RightKeys', 'ContCode');
% @2019.2.20这里需要处理：如果Suffix == 'CZC' 那么mainCont 改为3位数字
dataWarrant = trimCodeForCZC(dataWarrant);
windcode = rows2vars(dataWarrant(:, {'mainCont', 'Suffix'}));
windcode(:, 1) = [];
windcode = varfun(@(x) {horzcat(x{1}, '.', x{2})}, windcode);
windcode = rows2vars(windcode);
dataWarrant.WindCode = windcode.Var1;
dataWarrant.Properties.VariableNames = {'Date', 'ContCode', 'MainCont', 'Suffix', 'WindCode'};

dataWarrant.Warrant = nan(height(dataWarrant), 1);
windCodeName = unique(dataWarrant(:, {'ContCode', 'WindCode'}));
%% 数据读取：
% intersect 读了20分钟
tic
for iDate = 1:length(unique(dataWarrant.Date))
    readDate = unique(dataWarrant.Date);
    readDate = readDate(iDate);
    readCode = dataWarrant(dataWarrant.Date == readDate, {'WindCode'});
     %  @2019.02.20终于遇到了这个坑。。。因为700001这列数不是按照品种字母顺序排序的，整体是，但不知道为啥RI排在前面，导致用intersect的时候结果和原来顺序不一样
    dataI = table(repmat(readDate, height(readCode), 1), readCode.WindCode);
    dataI.Properties.VariableNames = {'Date', 'WindCode'};
    % 这里需要对dataI
    % attach上ContCode，这样用code去做intersect的key，就可以保证出来的结果是按照700001排序而不是按照首字母
    dataI = outerjoin(dataI, windCodeName, 'type', 'left', 'MergeKeys', true, ...
        'LeftKeys', 'WindCode', 'RightKeys', 'WindCode');
    % 这里对dataI按照ContCode排序，才能保证w_wss_data读出来的数是按Code排序的可以直接赋值到dataWarrant上
    dataI = sortrows(dataI, 'ContCode');
    str = sprintf(...
        '[w_wss_data,~,~,~,w_wss_errorid,~]=w.wss(char(join(dataI.WindCode, '','')),''st_stock'',''tradeDate=%s'');', num2str(readDate));
    eval(str)
    if w_wss_errorid ~= 0
        error('Wind Data Error!')
    end
   
    [~, idx, ~] = intersect(dataWarrant(:, {'Date', 'ContCode'}), dataI(:, {'Date', 'ContCode'}), 'rows');
    dataWarrant.Warrant(idx) = w_wss_data;
    
end
toc
% 
% % 读取所有合约的所有日期
% dateFrom = min(dataWarrant.Date);
% dateTo = max(dataWarrant.Date);
% readCode = unique(dataWarrant.WindCode);
% readCode = join(readCode, ',');
% str = sprintf(...
%     '[w_wsd_data,w_wsd_codes,~,w_wsd_times,w_wsd_errorid,~]=w.wsd(''%s'',''st_stock'',''%s'',''%s'');', char(readCode), num2str(dateFrom), num2str(dateTo));
% tic
% eval(str)
% if w_wsd_errorid ~= 0
%     error('Wind Data Error!')
% end
% toc
% 
% % 读的合约数少没问题，全部读的话会报错。。why？




% 调整成统一格式保存
dataWarrant = dataWarrant(:, {'Date', 'ContCode', 'Warrant'});
save('E:\futureData\dataWarrant.mat', 'dataWarrant');











