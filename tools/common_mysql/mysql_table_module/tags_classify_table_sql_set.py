# 标签分类相关表的sql语句集合
# ================ 相关表的sql语句集合 ================
gitee_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(512) not null comment 'purl作为唯一索引',
    url_name VARCHAR(512) not null comment '用户/组件名称',
    homepage VARCHAR(512) not null comment '官网地址',
    description TEXT comment '描述',
    gitee_tags TEXT comment 'gitee标签',
    component_dim_tags TEXT comment '组件域标签',
    sub_dim_tags TEXT comment '细分域分类标签',
    reason_field TEXT comment '分类根据字段',
    reason_content TEXT comment '分类原因',
    created_time DATETIME not null DEFAULT CURRENT_TIMESTAMP comment '创建时间',
    updated_time DATETIME not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment '更新时间',
    primary key (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Gitee标签分类结果';
"""

atomgit_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(512) not null comment 'purl作为唯一索引',
    url_name VARCHAR(512) not null comment '用户/组件名称',
    homepage VARCHAR(512) not null comment '官网地址',
    description TEXT comment '描述',
    atomgit_tags TEXT comment 'atomgit标签',
    component_dim_tags TEXT comment '组件域标签',
    sub_dim_tags TEXT comment '细分域分类标签',
    reason_field TEXT comment '分类根据字段',
    reason_content TEXT comment '分类原因',
    created_time DATETIME not null DEFAULT CURRENT_TIMESTAMP comment '创建时间',
    updated_time DATETIME not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment '更新时间',
    primary key (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Atomgit标签分类结果';
"""

gitlab_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(512) not null comment 'purl作为唯一索引',
    url_name VARCHAR(512) not null comment '用户/组件名称',
    homepage VARCHAR(512) not null comment '官网地址',
    description TEXT comment '描述',
    gitlab_tags TEXT comment 'gitlab标签',
    component_dim_tags TEXT comment '组件域标签',
    sub_dim_tags TEXT comment '细分域分类标签',
    reason_field TEXT comment '分类根据字段',
    reason_content TEXT comment '分类原因',
    created_time DATETIME not null DEFAULT CURRENT_TIMESTAMP comment '创建时间',
    updated_time DATETIME not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment '更新时间',
    primary key (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Gitlab标签分类结果';
"""

github_tags_classify_result = """
    CREATE TABLE if not exists {database_name}.{table_name}(
    `repo_id` bigint NOT NULL COMMENT '仓库ID作为唯一索引',
    `purl` varchar(255) NOT NULL COMMENT 'purl',
    `url_name` varchar(255) NOT NULL COMMENT '用户/组件名称',
    `homepage` varchar(255) DEFAULT NULL COMMENT '官网地址',
    `description` text COMMENT '描述',
    `github_tags` text COMMENT 'github标签',
    `first_tags` text COMMENT '组件域标签',
    `classify_tags` text COMMENT '细分域分类标签',
    `reason_field` text COMMENT '分类根据字段',
    `reason_content` text COMMENT '分类原因',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`repo_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Github标签分类结果(带一级标签)';
"""

npm_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(255) NOT NULL COMMENT 'purl作为唯一索引',
    package_name VARCHAR(255) NOT NULL COMMENT '包名',
    homepage VARCHAR(255) DEFAULT NULL COMMENT '官网地址',
    repository VARCHAR(255) DEFAULT NULL COMMENT '源代码仓库地址',
    npm_keywords TEXT COMMENT 'npm关键词',
    first_tags TEXT COMMENT '组件域标签',
    classify_tags TEXT COMMENT '细分域分类标签',
    reason_field TEXT COMMENT '分类根据字段',
    reason_content TEXT COMMENT '分类原因',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Npm标签分类结果';
"""

npm_purl_html_bill_status = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    lower_purl   VARCHAR(255) DEFAULT NULL COMMENT '小写 purl / 包标识',
    is_finish    TINYINT(1) DEFAULT 0 COMMENT 'HTML 是否已爬取完成',
    status       VARCHAR(50) DEFAULT NULL COMMENT 'MinIO HTML 状态: 200/404/NULL(无页面)',
    is_status    TINYINT(1) DEFAULT 0 COMMENT '是否已执行 HTML 状态检测',
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY idx_lower_purl (lower_purl),
    KEY idx_is_status (is_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Npm purl HTML 账单状态';
"""

pypi_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(255) NOT NULL COMMENT 'purl作为唯一索引',
    project_name VARCHAR(255) NOT NULL COMMENT '用户/组件名称',
    homepage VARCHAR(255) DEFAULT NULL COMMENT '官网地址',
    github_url VARCHAR(255) DEFAULT NULL COMMENT 'github仓库地址',
    description TEXT COMMENT '描述',
    pypi_tags TEXT COMMENT 'pypi话题',
    first_tags TEXT COMMENT '组件域标签',
    classify_tags TEXT COMMENT '细分域分类标签',
    reason_field TEXT COMMENT '分类根据字段',
    reason_content TEXT COMMENT '分类原因',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Github标签分类结果';
"""

golang_tags_classify_result = """
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    purl VARCHAR(512) NOT NULL COMMENT 'purl作为唯一索引',
    url_name VARCHAR(512) NOT NULL COMMENT '用户/组件名称',
    repository VARCHAR(512) NOT NULL COMMENT '仓库地址',
    component_dim_tags TEXT COMMENT '组件域标签',
    sub_dim_tags TEXT COMMENT '细分域分类标签',
    reason_field TEXT COMMENT '分类根据字段',
    reason_content TEXT COMMENT '分类原因',
    created_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    is_finish TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否完成',
    PRIMARY KEY (purl)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Golang标签分类结果';
"""

# ========================================================
