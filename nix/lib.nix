{ lib, ... }:
let
  trimmed_string_both = s: lib.strings.trim s;
in
rec {
  mkGritPipeline =
    {
      name,
      steps ? [ ],
    }:
    trimmed_string_both (builtins.concatStringsSep "\n" steps);

  mkStep =
    args@{
      name,
      script,
      parallel,
      inputs ? [ ],
    }:
    let
      isAttrSet = a: builtins.hasAttr a args;
    in
    trimmed_string_both ''
      [[step]]
      name=\"${name}\"
      script=''''
      ${script}
      ''''
      ${if (isAttrSet "parallel") then "parallel=${builtins.toString parallel}" else ""}
      ${
        if (isAttrSet "inputs") then
          "inputs=[${builtins.concatStringsSep ", " (builtins.map (x: "\"${x}\"") inputs)}]"
        else
          ""
      }
    '';
}
