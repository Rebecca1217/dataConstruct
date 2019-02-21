function dataRes = trimCodeForCZC(dataInput)
%TRIMCODEFORCZC WindCode 在郑商所少一位
% 这个函数目前先不考虑复用问题，就是dataWarrant输入进来，能输出一个改造过mainCont的dataWarrant出去

% 当前数据是按照code_ContCode 和 date 排序的，下面的处理方式先把CZC挑出来处理，再合成
% 不用ifelse 的形式，因为rowfun不好用，麻烦

dataCZC = dataInput(strcmp(dataInput.Suffix, 'CZC'), :);
dataOther = dataInput(~strcmp(dataInput.Suffix, 'CZC'), :);

% dataCZC 需要把mainCont中年份的第一位数字去掉

mainContCharacter = regexp(dataCZC.mainCont, '^[A-Z]+', 'match');
mainContCharacter = cellfun(@char, mainContCharacter, 'UniformOutput', false);
mainContNum = regexp(dataCZC.mainCont, '[0-9]+', 'match');
mainContNum = cellfun(@(x) {x{1}(2:end)}, mainContNum);
mainContNew = join(horzcat(mainContCharacter, mainContNum), '');
dataCZC.mainCont = mainContNew;

dataRes = vertcat(dataCZC, dataOther);
dataRes = sortrows(dataRes, {'code_ContCode', 'date'});

end

