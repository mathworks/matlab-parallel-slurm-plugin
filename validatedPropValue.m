function val = validatedPropValue(ap, prop, type, defaultValue)
% If prop is in the AdditionalProperties ap, validate the value is the correct
% type and return it. If prop is not present, return the provided defaultValue.
% If prop is not present and no defaultValue is provided, returns empty.

% Copyright 2022 The MathWorks, Inc.

narginchk(3, 4);

if nargin < 4
    % If no defaultValue specified, use empty
    defaultValue = [];
end

if ~isprop(ap, prop)
    % prop is not present in ap, use the defaultValue
    val = defaultValue;
    return
end

% If we get here then prop is in ap
val = ap.(prop);
switch type
    case {'char', 'string'}
        validator = @(x) ischar(x) || isstring(x);
    case {'double', 'numeric'}
        validator = @isnumeric;
    case {'bool', 'logical'}
        validator = @islogical;
    otherwise
        error('parallelexamples:GenericSLURM:IncorrectArguments', ...
            'Not a valid data type');
end

% If the property is not empty, verify that it is set to the correct type:
% char, double, or logical.
if ~isempty(val) && ~validator(val)
    error('parallelexamples:GenericSLURM:IncorrectArguments', ...
        'Expected property ''%s'' to be of type %s, but it has type %s.', prop, type, class(val));
end

end
