# 加载 Profile 后出现 (base)，再切换到 dev_ver_n
if ($env:CONDA_DEFAULT_ENV -eq 'dev_ver_n') {
    return
}

& 'D:\Anaconda\shell\condabin\conda-hook.ps1'
conda activate 'dev_ver_n'
