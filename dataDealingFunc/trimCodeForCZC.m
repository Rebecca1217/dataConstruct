function dataRes = trimCodeForCZC(dataInput)
%TRIMCODEFORCZC WindCode ��֣������һλ
% �������Ŀǰ�Ȳ����Ǹ������⣬����dataWarrant��������������һ�������mainCont��dataWarrant��ȥ

% ��ǰ�����ǰ���code_ContCode �� date ����ģ�����Ĵ���ʽ�Ȱ�CZC�����������ٺϳ�
% ����ifelse ����ʽ����Ϊrowfun�����ã��鷳

dataCZC = dataInput(strcmp(dataInput.Suffix, 'CZC'), :);
dataOther = dataInput(~strcmp(dataInput.Suffix, 'CZC'), :);

% dataCZC ��Ҫ��mainCont����ݵĵ�һλ����ȥ��

mainContCharacter = regexp(dataCZC.mainCont, '^[A-Z]+', 'match');
mainContCharacter = cellfun(@char, mainContCharacter, 'UniformOutput', false);
mainContNum = regexp(dataCZC.mainCont, '[0-9]+', 'match');
mainContNum = cellfun(@(x) {x{1}(2:end)}, mainContNum);
mainContNew = join(horzcat(mainContCharacter, mainContNum), '');
dataCZC.mainCont = mainContNew;

dataRes = vertcat(dataCZC, dataOther);
dataRes = sortrows(dataRes, {'code_ContCode', 'date'});

end

