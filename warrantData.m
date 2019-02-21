% warrantData  ע��ֵ�����
% [w_wss_data,w_wss_codes,w_wss_fields,w_wss_times,w_wss_errorid,w_wss_reqid]=w.wss('CU1906.SHF,CU1907.SHF,CU1908.SHF,CU1909.SHF','st_stock','tradeDate=20190217')
addpath dataDealingFunc
%% ����ϸ��
% 1��ע��ֵ�����ֻѡ��������Լ����Ҫ��׼��ÿ��Ʒ�ֵĵ�ǰ������Լ���룻��Դ����ѩTableData
% 2����WSS��ȡÿ����Լÿ������ݣ���Ҫѭ�����ڣ�����20190105��Ҫ20���ӣ�ע��ֵ��ǰ�Ʒ�����֣�ͬһ��ͬƷ�ֵ����к�Լע��ֵ���һ��

% 3��֣������ÿ��Ʒ��Wind���붼��һλ����

%% ����׼����
contPath = 'Z:\baseData\TableData\futureData\TableData.mat';
load(contPath);
dataWarrant = TableData(:, {'date', 'code', 'mainCont'});
load('para\codeName.mat')

w = windmatlab;
dataWarrant = outerjoin(dataWarrant, codeName(:, {'ContCode', 'Suffix'}), 'type', 'left', 'MergeKeys', true, ...
    'LeftKeys', 'code', 'RightKeys', 'ContCode');
% @2019.2.20������Ҫ�������Suffix == 'CZC' ��ômainCont ��Ϊ3λ����
dataWarrant = trimCodeForCZC(dataWarrant);
windcode = rows2vars(dataWarrant(:, {'mainCont', 'Suffix'}));
windcode(:, 1) = [];
windcode = varfun(@(x) {horzcat(x{1}, '.', x{2})}, windcode);
windcode = rows2vars(windcode);
dataWarrant.WindCode = windcode.Var1;
dataWarrant.Properties.VariableNames = {'Date', 'ContCode', 'MainCont', 'Suffix', 'WindCode'};

dataWarrant.Warrant = nan(height(dataWarrant), 1);
windCodeName = unique(dataWarrant(:, {'ContCode', 'WindCode'}));
%% ���ݶ�ȡ��
% intersect ����20����
tic
for iDate = 1:length(unique(dataWarrant.Date))
    readDate = unique(dataWarrant.Date);
    readDate = readDate(iDate);
    readCode = dataWarrant(dataWarrant.Date == readDate, {'WindCode'});
     %  @2019.02.20��������������ӡ�������Ϊ700001���������ǰ���Ʒ����ĸ˳������ģ������ǣ�����֪��ΪɶRI����ǰ�棬������intersect��ʱ������ԭ��˳��һ��
    dataI = table(repmat(readDate, height(readCode), 1), readCode.WindCode);
    dataI.Properties.VariableNames = {'Date', 'WindCode'};
    % ������Ҫ��dataI
    % attach��ContCode��������codeȥ��intersect��key���Ϳ��Ա�֤�����Ľ���ǰ���700001��������ǰ�������ĸ
    dataI = outerjoin(dataI, windCodeName, 'type', 'left', 'MergeKeys', true, ...
        'LeftKeys', 'WindCode', 'RightKeys', 'WindCode');
    % �����dataI����ContCode���򣬲��ܱ�֤w_wss_data�����������ǰ�Code����Ŀ���ֱ�Ӹ�ֵ��dataWarrant��
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
% % ��ȡ���к�Լ����������
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
% % ���ĺ�Լ����û���⣬ȫ�����Ļ��ᱨ����why��




% ������ͳһ��ʽ����
dataWarrant = dataWarrant(:, {'Date', 'ContCode', 'Warrant'});
save('E:\futureData\dataWarrant.mat', 'dataWarrant');











