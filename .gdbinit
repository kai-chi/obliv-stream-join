#enable SGX measuring tool
enable sgx_emmt

#exit GDB after succesfull execution
set $_exitcode = -999
define hook-stop
  if $_exitcode != -999
    quit
  end
end

run
continue
continue
continue
continue