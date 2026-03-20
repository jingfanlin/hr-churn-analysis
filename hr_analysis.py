import sqlite3
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 第一部分：创建数据库和表结构
# ============================================

def create_database():
    """创建数据库和所有表"""
    # 连接数据库（如果不存在会自动创建）
    conn = sqlite3.connect('hr_analysis.db')
    cursor = conn.cursor()
    
    print("=" * 60)
    print("正在创建数据库和表结构...")
    print("=" * 60)
    
    # 1. 创建部门表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            department_id INTEGER PRIMARY KEY,
            department_name TEXT NOT NULL,
            manager_id INTEGER,
            location TEXT
        )
    """)
    # 逐行解释：
    # - CREATE TABLE IF NOT EXISTS: 如果表不存在才创建，避免重复创建报错
    # - INTEGER PRIMARY KEY: 整数类型的主键，在 SQLite 中会自动作为 ROWID
    # - TEXT NOT NULL: 文本类型，不能为空
    # - manager_id 和 location 可以为空（没有 NOT NULL 约束）
    
    # 2. 创建职位表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY,
            position_name TEXT NOT NULL,
            level INTEGER,
            min_salary REAL,
            max_salary REAL
        )
    """)
    # 逐行解释：
    # - REAL: 浮点数类型，用于存储薪资（SQLite 中没有 DECIMAL，用 REAL 代替）
    # - level: 职级，用整数表示（1-10）
    
    # 3. 创建员工表（核心表）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            gender TEXT,
            birth_date DATE,
            hire_date DATE,
            department_id INTEGER,
            position_id INTEGER,
            salary REAL,
            performance_score REAL,
            status TEXT DEFAULT '在职',
            FOREIGN KEY (department_id) REFERENCES departments(department_id),
            FOREIGN KEY (position_id) REFERENCES positions(position_id)
        )
    """)
    # 逐行解释：
    # - DATE: 日期类型，格式为 'YYYY-MM-DD'
    # - DEFAULT '在职': 默认值，插入数据时不指定 status 则自动填充'在职'
    # - FOREIGN KEY: 外键约束，确保 department_id 必须在 departments 表中存在
    # - 外键的作用：保证数据完整性，防止引用不存在的部门或职位
    
    # 4. 创建薪资调整记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS salary_adjustments (
            adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER,
            old_salary REAL,
            new_salary REAL,
            adjustment_date DATE,
            adjustment_rate REAL,
            reason TEXT,
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)
    # 逐行解释：
    # - AUTOINCREMENT: 自动递增，插入数据时不需要指定 adjustment_id
    # - adjustment_rate: 调整幅度（百分比），例如 15.50 表示涨薪 15.5%
    
    # 5. 创建离职记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resignations (
            resignation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER,
            resignation_date DATE,
            reason TEXT,
            work_years REAL,
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)
    # 逐行解释：
    # - work_years: 工作年限，例如 2.50 表示工作了2年6个月
    # - 这个表记录历史离职信息，即使员工数据删除，离职记录仍保留
    
    conn.commit()
    print("✓ 数据库和表结构创建成功！\n")
    return conn


# ============================================
# 第二部分：插入测试数据
# ============================================

def insert_test_data(conn):
    """插入测试数据"""
    cursor = conn.cursor()
    
    print("=" * 60)
    print("正在插入测试数据...")
    print("=" * 60)
    
    # 1. 插入部门数据
    departments_data = [
        (1, '技术部', 101, '深圳'),
        (2, '产品部', 102, '深圳'),
        (3, '市场部', 103, '北京'),
        (4, '销售部', 104, '上海'),
        (5, '人力资源部', 105, '深圳'),
        (6, '财务部', 106, '深圳')
    ]
    cursor.executemany(
        'INSERT OR IGNORE INTO departments VALUES (?, ?, ?, ?)',
        departments_data
    )
    # 逐行解释：
    # - executemany: 批量插入多行数据，比循环 execute 效率高
    # - INSERT OR IGNORE: 如果主键冲突就忽略，避免重复插入报错
    # - ?: 占位符，对应 departments_data 中每个元组的值
    
    # 2. 插入职位数据
    positions_data = [
        (1, 'Java开发工程师', 3, 8000, 15000),
        (2, 'Python开发工程师', 3, 8000, 15000),
        (3, '前端开发工程师', 3, 7000, 13000),
        (4, '数据分析师', 3, 7000, 14000),
        (5, '产品经理', 4, 10000, 20000),
        (6, '高级产品经理', 6, 15000, 30000),
        (7, '市场专员', 2, 5000, 8000),
        (8, '销售代表', 2, 6000, 12000),
        (9, 'HR专员', 3, 6000, 10000),
        (10, '财务专员', 3, 6000, 10000)
    ]
    cursor.executemany(
        'INSERT OR IGNORE INTO positions VALUES (?, ?, ?, ?, ?)',
        positions_data
    )
    
    # 3. 插入员工数据（30条）
    employees_data = [
        (101, '张伟', '男', '1988-05-12', '2018-03-15', 1, 1, 12000, 4.2, '在职'),
        (102, '李娜', '女', '1992-08-20', '2019-06-01', 1, 2, 11000, 4.5, '在职'),
        (103, '王强', '男', '1990-11-03', '2017-01-10', 1, 1, 14000, 3.8, '在职'),
        (104, '刘芳', '女', '1995-02-14', '2020-09-01', 2, 5, 15000, 4.6, '在职'),
        (105, '陈明', '男', '1987-07-25', '2016-05-20', 5, 9, 8000, 4.0, '在职'),
        (106, '赵丽', '女', '1993-12-08', '2021-01-15', 3, 7, 6500, 3.9, '在职'),
        (107, '孙杰', '男', '1991-04-17', '2019-11-01', 4, 8, 9000, 4.3, '在职'),
        (108, '周敏', '女', '1989-09-30', '2018-07-10', 2, 5, 16000, 4.7, '在职'),
        (109, '吴涛', '男', '1994-06-22', '2020-03-01', 1, 3, 10000, 4.1, '在职'),
        (110, '郑红', '女', '1996-01-05', '2021-08-20', 6, 10, 8500, 4.4, '在职'),
        (111, '钱龙', '男', '1986-10-11', '2015-02-01', 1, 1, 13500, 3.7, '离职'),
        (112, '孙燕', '女', '1993-03-28', '2019-12-15', 3, 7, 7000, 4.2, '在职'),
        (113, '李伟', '男', '1990-07-19', '2017-09-01', 4, 8, 11000, 4.5, '在职'),
        (114, '王敏', '女', '1992-11-24', '2020-01-10', 2, 5, 14000, 4.3, '在职'),
        (115, '张强', '男', '1988-05-07', '2016-08-15', 1, 2, 12500, 3.9, '在职'),
        (116, '刘静', '女', '1995-09-13', '2021-04-01', 5, 9, 7500, 4.1, '在职'),
        (117, '陈涛', '男', '1991-02-28', '2018-10-20', 4, 8, 9500, 4.0, '在职'),
        (118, '赵敏', '女', '1994-12-16', '2020-07-01', 3, 7, 6800, 3.8, '在职'),
        (119, '孙强', '男', '1987-08-09', '2015-11-01', 1, 1, 15000, 4.6, '在职'),
        (120, '周莉', '女', '1993-04-21', '2019-05-15', 2, 6, 18000, 4.8, '在职'),
        (121, '吴刚', '男', '1989-10-03', '2017-03-01', 1, 2, 11500, 4.2, '离职'),
        (122, '郑敏', '女', '1996-06-15', '2021-09-10', 6, 10, 8000, 4.3, '在职'),
        (123, '钱伟', '男', '1990-01-27', '2018-12-01', 4, 8, 10500, 4.4, '在职'),
        (124, '孙丽', '女', '1992-07-08', '2020-02-20', 3, 7, 7200, 4.0, '在职'),
        (125, '李静', '女', '1994-11-19', '2021-06-01', 5, 9, 7800, 4.1, '在职'),
        (126, '王涛', '男', '1988-03-12', '2016-09-15', 1, 1, 13000, 3.8, '在职'),
        (127, '张敏', '女', '1991-09-24', '2019-01-10', 2, 5, 15500, 4.5, '在职'),
        (128, '刘强', '男', '1995-05-06', '2020-11-01', 4, 8, 9200, 4.2, '在职'),
        (129, '陈丽', '女', '1993-01-18', '2021-03-15', 3, 7, 6600, 3.9, '在职'),
        (130, '赵刚', '男', '1987-12-30', '2015-07-01', 1, 2, 14500, 4.7, '在职')
    ]
    cursor.executemany(
        'INSERT OR IGNORE INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        employees_data
    )
    # 逐行解释：
    # - 每个元组包含10个值，对应 employees 表的10个字段
    # - 日期格式必须是 'YYYY-MM-DD' 字符串
    # - 注意员工111和121的 status 是'离职'，用于测试离职分析
    
    # 4. 插入薪资调整记录
    salary_adjustments_data = [
        (1, 101, 10000, 12000, '2022-01-01', 20.00, '年度调薪'),
        (2, 102, 9000, 11000, '2022-01-01', 22.22, '年度调薪'),
        (3, 104, 12000, 15000, '2021-06-01', 25.00, '晋升加薪'),
        (4, 108, 14000, 16000, '2022-01-01', 14.29, '年度调薪'),
        (5, 119, 13000, 15000, '2021-01-01', 15.38, '绩效优秀'),
        (6, 120, 15000, 18000, '2021-12-01', 20.00, '晋升加薪'),
        (7, 103, 12000, 14000, '2022-01-01', 16.67, '年度调薪'),
        (8, 127, 13000, 15500, '2022-01-01', 19.23, '年度调薪')
    ]
    cursor.executemany(
        'INSERT OR IGNORE INTO salary_adjustments VALUES (?, ?, ?, ?, ?, ?, ?)',
        salary_adjustments_data
    )
    # 逐行解释：
    # - adjustment_rate 的计算公式：(new_salary - old_salary) / old_salary * 100
    # - 例如第1条：(12000 - 10000) / 10000 * 100 = 20.00%
    
    # 5. 插入离职记录
    resignations_data = [
        (1, 111, '2023-06-30', '个人发展', 8.41),
        (2, 121, '2023-03-15', '薪资原因', 6.04)
    ]
    cursor.executemany(
        'INSERT OR IGNORE INTO resignations VALUES (?, ?, ?, ?, ?)',
        resignations_data
    )
    # 逐行解释：
    # - work_years 是从入职到离职的时间跨度（年）
    # - 员工111：2015-02-01 到 2023-06-30，约 8.41 年
    
    conn.commit()
    print("✓ 测试数据插入成功！\n")


# ============================================
# 第三部分：数据分析查询
# ============================================

def analysis_1_department_structure(conn):
    """分析1：各部门人员分布"""
    print("=" * 60)
    print("分析1：各部门人员分布")
    print("=" * 60)
    
    query = """
        SELECT 
            d.department_name AS 部门名称,
            COUNT(e.employee_id) AS 在职人数,
            ROUND(AVG(e.salary), 2) AS 平均薪资,
            ROUND(AVG(e.performance_score), 2) AS 平均绩效,
            MIN(e.salary) AS 最低薪资,
            MAX(e.salary) AS 最高薪资
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
        WHERE e.status = '在职'
        GROUP BY d.department_name
        ORDER BY 在职人数 DESC
    """
    # 逐行解释：
    # 1. SELECT 子句：
    #    - d.department_name AS 部门名称: 从 departments 表获取部门名称，并重命名
    #    - COUNT(e.employee_id): 统计每个部门的员工数量
    #    - ROUND(AVG(e.salary), 2): 计算平均薪资，ROUND 四舍五入到2位小数
    #    - AVG(): 聚合函数，计算平均值
    #    - MIN() 和 MAX(): 分别获取最小值和最大值
    
    # 2. FROM employees e:
    #    - 主表是 employees（员工表）
    #    - e 是表的别名，方便后续引用
    
    # 3. INNER JOIN departments d ON e.department_id = d.department_id:
    #    - INNER JOIN: 内连接，只返回两表都匹配的记录
    #    - ON: 连接条件，通过 department_id 关联两表
    #    - 作用：获取每个员工对应的部门名称
    
    # 4. WHERE e.status = '在职':
    #    - 筛选条件，只统计在职员工
    #    - 注意：WHERE 在 GROUP BY 之前执行，先筛选再分组
    
    # 5. GROUP BY d.department_name:
    #    - 按部门名称分组，每个部门一行结果
    #    - GROUP BY 之后，SELECT 中只能出现：
    #      a) GROUP BY 的字段
    #      b) 聚合函数（COUNT, AVG, SUM等）
    
    # 6. ORDER BY 在职人数 DESC:
    #    - 按在职人数降序排列（DESC = 降序，ASC = 升序）
    #    - 可以用列的别名（在职人数）或列的位置（2）
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    print("\n")
    return df


def analysis_2_gender_age_distribution(conn):
    """分析2：性别和年龄分布"""
    print("=" * 60)
    print("分析2：性别和年龄分布")
    print("=" * 60)
    
    query = """
        SELECT 
            gender AS 性别,
            COUNT(*) AS 人数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM employees WHERE status = '在职'), 2) AS 占比,
            ROUND(AVG((JULIANDAY('now') - JULIANDAY(birth_date)) / 365.25), 1) AS 平均年龄,
            MIN((JULIANDAY('now') - JULIANDAY(birth_date)) / 365.25) AS 最小年龄,
            MAX((JULIANDAY('now') - JULIANDAY(birth_date)) / 365.25) AS 最大年龄
        FROM employees
        WHERE status = '在职'
        GROUP BY gender
    """
    # 逐行解释：
    # 1. COUNT(*) * 100.0 / (SELECT COUNT(*) FROM employees WHERE status = '在职'):
    #    - COUNT(*) * 100.0: 当前分组的人数乘以100（.0确保浮点数运算）
    #    - (SELECT ...): 子查询，获取总在职人数
    #    - 除法得到百分比
    #    - 例如：男性10人，总共28人 → 10 * 100.0 / 28 = 35.71%
    
    # 2. (JULIANDAY('now') - JULIANDAY(birth_date)) / 365.25:
    #    - JULIANDAY(): SQLite 的日期函数，将日期转换为儒略日（天数）
    #    - 'now': 当前日期
    #    - 两个日期相减得到天数差
    #    - 除以 365.25（考虑闰年）得到年龄
    #    - ROUND(..., 1): 保留1位小数
    
    # 3. 为什么用 COUNT(*) 而不是 COUNT(gender)?
    #    - COUNT(*): 统计所有行，包括 NULL
    #    - COUNT(gender): 只统计 gender 非 NULL 的行
    #    - 这里两者结果相同，但 COUNT(*) 更常用
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    print("\n")
    return df


def analysis_3_salary_structure(conn):
    """分析3：薪资结构分析"""
    print("=" * 60)
    print("分析3：薪资结构分析（按薪资区间统计）")
    print("=" * 60)
    
    query = """
        SELECT 
            CASE 
                WHEN salary < 7000 THEN '6000以下'
                WHEN salary < 10000 THEN '7000-10000'
                WHEN salary < 15000 THEN '10000-15000'
                ELSE '15000以上'
            END AS 薪资区间,
            COUNT(*) AS 人数,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM employees WHERE status = '在职'), 2) AS 占比
        FROM employees
        WHERE status = '在职'
        GROUP BY 
            CASE 
                WHEN salary < 7000 THEN '6000以下'
                WHEN salary < 10000 THEN '7000-10000'
                WHEN salary < 15000 THEN '10000-15000'
                ELSE '15000以上'
            END
        ORDER BY 
            MIN(salary)
    """
    # 逐行解释：
    # 1. CASE WHEN ... THEN ... ELSE ... END:
    #    - CASE 表达式：根据条件返回不同的值（类似 if-else）
    #    - WHEN salary < 7000 THEN '6000以下': 如果薪资小于7000，返回'6000以下'
    #    - WHEN salary < 10000 THEN '7000-10000': 如果薪资小于10000（且>=7000），返回'7000-10000'
    #    - ELSE '15000以上': 其他情况（薪资>=15000）
    #    - END: CASE 表达式结束标志
    #    - AS 薪资区间: 给计算结果取别名
    
    # 2. 为什么 GROUP BY 要重复一遍 CASE 表达式？
    #    - SQLite 不支持 GROUP BY 用 SELECT 中定义的别名
    #    - 必须完整写一遍 CASE 表达式
    #    - 注意：MySQL 支持 GROUP BY 薪资区间（用别名）
    
    # 3. ORDER BY MIN(salary):
    #    - 按每个薪资区间的最小值排序
    #    - 确保区间按薪资从低到高排列
    #    - 也可以用 ORDER BY 1（按第1列排序）
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    print("\n")
    return df


def analysis_4_work_years(conn):
    """分析4：员工工龄分析"""
    print("=" * 60)
    print("分析4：员工工龄分析")
    print("=" * 60)
    
    query = """
        SELECT 
            name AS 姓名,
            d.department_name AS 部门,
            hire_date AS 入职日期,
            ROUND((JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25, 1) AS 工作年限,
            salary AS 当前薪资
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
        WHERE e.status = '在职'
        ORDER BY 工作年限 DESC
        LIMIT 10
    """
    # 逐行解释：
    # 1. ROUND((JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25, 1):
    #    - JULIANDAY('now'): 今天的儒略日
    #    - JULIANDAY(hire_date): 入职日期的儒略日
    #    - 相减得到工作天数
    #    - 除以 365.25 得到工作年数（考虑闰年）
    #    - ROUND(..., 1): 保留1位小数
    
    # 2. LIMIT 10:
    #    - 限制结果只返回前10行
    #    - 结合 ORDER BY，可以获取"TOP 10"
    #    - 注意：SQLite 不支持 TOP 关键字，要用 LIMIT
    
    # 3. INNER JOIN 的顺序：
    #    - FROM employees e: 主表
    #    - INNER JOIN departments d: 连接的表
    #    - 可以有多个 JOIN，按顺序执行
    
    df = pd.read_sql_query(query, conn)
    print("【工龄 TOP 10 员工】")
    print(df.to_string(index=False))
    print("\n")
    
    # 工龄分布统计
    query2 = """
        SELECT 
            CASE 
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 1 THEN '1年以内'
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 3 THEN '1-3年'
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 5 THEN '3-5年'
                ELSE '5年以上'
            END AS 工龄区间,
            COUNT(*) AS 人数,
            ROUND(AVG(salary), 2) AS 平均薪资
        FROM employees
        WHERE status = '在职'
        GROUP BY 
            CASE 
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 1 THEN '1年以内'
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 3 THEN '1-3年'
                WHEN (JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25 < 5 THEN '3-5年'
                ELSE '5年以上'
            END
    """
    # 逐行解释：
    # - 这个查询和分析3的逻辑类似
    # - 区别在于：这里用日期计算工龄，然后分区间统计
    # - 可以看出工龄和薪资的关系（通常工龄越长，平均薪资越高）
    
    df2 = pd.read_sql_query(query2, conn)
    print("【工龄分布统计】")
    print(df2.to_string(index=False))
    print("\n")
    return df, df2


def analysis_5_resignation(conn):
    """分析5：离职率分析"""
    print("=" * 60)
    print("分析5：离职率分析")
    print("=" * 60)
    
    # 5.1 整体离职率
    query1 = """
        SELECT 
            COUNT(CASE WHEN status = '在职' THEN 1 END) AS 在职人数,
            COUNT(CASE WHEN status = '离职' THEN 1 END) AS 离职人数,
            COUNT(*) AS 总人数,
            ROUND(COUNT(CASE WHEN status = '离职' THEN 1 END) * 100.0 / COUNT(*), 2) AS 离职率
        FROM employees
    """
    # 逐行解释：
    # 1. COUNT(CASE WHEN status = '在职' THEN 1 END):
    #    - CASE WHEN: 条件判断
    #    - WHEN status = '在职' THEN 1: 如果在职，返回1
    #    - END: 如果不在职，返回 NULL（没有 ELSE 子句时默认 NULL）
    #    - COUNT(...): 统计非 NULL 的值，即统计在职人数
    #    - 等价于：COUNT(*) WHERE status = '在职'，但更灵活
    
    # 2. 为什么用 COUNT(CASE ...) 而不是两次查询？
    #    - 一次查询可以同时得到在职和离职人数
    #    - 性能更好，代码更简洁
    
    # 3. * 100.0:
    #    - 100 是整数，100.0 是浮点数
    #    - 确保除法是浮点数运算，否则会得到整数结果
    #    - 例如：5 / 2 = 2，5 / 2.0 = 2.5
    
    df1 = pd.read_sql_query(query1, conn)
    print("【整体离职情况】")
    print(df1.to_string(index=False))
    print("\n")
    
    # 5.2 各部门离职率
    query2 = """
        SELECT 
            d.department_name AS 部门名称,
            COUNT(CASE WHEN e.status = '在职' THEN 1 END) AS 在职人数,
            COUNT(CASE WHEN e.status = '离职' THEN 1 END) AS 离职人数,
            ROUND(COUNT(CASE WHEN e.status = '离职' THEN 1 END) * 100.0 / COUNT(*), 2) AS 离职率
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
        GROUP BY d.department_name
        ORDER BY 离职率 DESC
    """
    # 逐行解释：
    # - 这个查询和上面的逻辑相同
    # - 区别在于加了 GROUP BY d.department_name
    # - 每个部门单独计算离职率
    # - ORDER BY 离职率 DESC: 按离职率从高到低排序，找出问题部门
    
    df2 = pd.read_sql_query(query2, conn)
    print("【各部门离职率】")
    print(df2.to_string(index=False))
    print("\n")
    
    # 5.3 离职原因分析
    query3 = """
        SELECT 
            reason AS 离职原因,
            COUNT(*) AS 人数,
            ROUND(AVG(work_years), 2) AS 平均工作年限,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM resignations), 2) AS 占比
        FROM resignations
        GROUP BY reason
        ORDER BY 人数 DESC
    """
    # 逐行解释：
    # - 从 resignations 表获取离职原因
    # - AVG(work_years): 计算每种原因的平均工作年限
    # - 可以看出：是新员工容易离职，还是老员工离职？
    # - (SELECT COUNT(*) FROM resignations): 子查询，获取总离职人数
    
    df3 = pd.read_sql_query(query3, conn)
    print("【离职原因分析】")
    print(df3.to_string(index=False))
    print("\n")
    
    return df1, df2, df3


def analysis_6_salary_adjustment(conn):
    """分析6：薪资调整分析"""
    print("=" * 60)
    print("分析6：薪资调整分析")
    print("=" * 60)
    
    # 6.1 薪资调整记录
    query1 = """
        SELECT 
            e.name AS 姓名,
            d.department_name AS 部门,
            sa.old_salary AS 调整前,
            sa.new_salary AS 调整后,
            sa.adjustment_rate AS 涨幅,
            sa.adjustment_date AS 调整日期,
            sa.reason AS 原因
        FROM salary_adjustments sa
        INNER JOIN employees e ON sa.employee_id = e.employee_id
        INNER JOIN departments d ON e.department_id = d.department_id
        ORDER BY sa.adjustment_date DESC, sa.adjustment_rate DESC
    """
    # 逐行解释：
    # 1. 多表连接：
    #    - FROM salary_adjustments sa: 主表（薪资调整表）
    #    - INNER JOIN employees e: 连接员工表，获取姓名
    #    - INNER JOIN departments d: 再连接部门表，获取部门名称
    #    - 连接是链式的：sa → e → d
    
    # 2. ORDER BY sa.adjustment_date DESC, sa.adjustment_rate DESC:
    #    - 多列排序：先按日期降序，再按涨幅降序
    #    - 逗号分隔多个排序条件
    #    - 执行顺序：先按第1列排序，第1列相同时按第2列排序
    
    df1 = pd.read_sql_query(query1, conn)
    print("【薪资调整记录】")
    print(df1.to_string(index=False))
    print("\n")
    
    # 6.2 按原因统计调薪情况
    query2 = """
        SELECT 
            reason AS 调薪原因,
            COUNT(*) AS 人数,
            ROUND(AVG(adjustment_rate), 2) AS 平均涨幅,
            MIN(adjustment_rate) AS 最小涨幅,
            MAX(adjustment_rate) AS 最大涨幅
        FROM salary_adjustments
        GROUP BY reason
        ORDER BY 人数 DESC
    """
    # 逐行解释：
    # - 按调薪原因分组统计
    # - 可以看出：哪种原因调薪最多？涨幅多少？
    # - 常见原因："年度调薪"、"晋升加薪"、"绩效优秀"
    
    df2 = pd.read_sql_query(query2, conn)
    print("【按原因统计调薪情况】")
    print(df2.to_string(index=False))
    print("\n")
    
    return df1, df2


def analysis_7_performance(conn):
    """分析7：绩效分析"""
    print("=" * 60)
    print("分析7：绩效分析")
    print("=" * 60)
    
    # 7.1 绩效分布
    query1 = """
        SELECT 
            CASE 
                WHEN performance_score >= 4.5 THEN '优秀(4.5-5.0)'
                WHEN performance_score >= 4.0 THEN '良好(4.0-4.5)'
                WHEN performance_score >= 3.5 THEN '合格(3.5-4.0)'
                ELSE '待改进(<3.5)'
            END AS 绩效等级,
            COUNT(*) AS 人数,
            ROUND(AVG(salary), 2) AS 平均薪资,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM employees WHERE status = '在职'), 2) AS 占比
        FROM employees
        WHERE status = '在职'
        GROUP BY 
            CASE 
                WHEN performance_score >= 4.5 THEN '优秀(4.5-5.0)'
                WHEN performance_score >= 4.0 THEN '良好(4.0-4.5)'
                WHEN performance_score >= 3.5 THEN '合格(3.5-4.0)'
                ELSE '待改进(<3.5)'
            END
        ORDER BY MIN(performance_score) DESC
    """
    # 逐行解释：
    # - 使用 CASE 表达式将连续的绩效分数分成4个等级
    # - 每个等级统计人数和平均薪资
    # - 可以看出：绩效和薪资的关系
    # - ORDER BY MIN(performance_score) DESC: 按绩效从高到低排序
    
    df1 = pd.read_sql_query(query1, conn)
    print("【绩效分布】")
    print(df1.to_string(index=False))
    print("\n")
    
    # 7.2 各部门绩效对比
    query2 = """
        SELECT 
            d.department_name AS 部门,
            COUNT(*) AS 人数,
            ROUND(AVG(e.performance_score), 2) AS 平均绩效,
            MIN(e.performance_score) AS 最低绩效,
            MAX(e.performance_score) AS 最高绩效,
            COUNT(CASE WHEN e.performance_score >= 4.5 THEN 1 END) AS 优秀人数,
            ROUND(COUNT(CASE WHEN e.performance_score >= 4.5 THEN 1 END) * 100.0 / COUNT(*), 2) AS 优秀率
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
        WHERE e.status = '在职'
        GROUP BY d.department_name
        ORDER BY 平均绩效 DESC
    """
    # 逐行解释：
    # - 各部门的绩效统计
    # - COUNT(CASE WHEN e.performance_score >= 4.5 THEN 1 END): 统计优秀人数
    # - 优秀率 = 优秀人数 / 总人数 * 100
    # - 可以看出：哪个部门绩效最好？哪个部门需要改进？
    
    df2 = pd.read_sql_query(query2, conn)
    print("【各部门绩效对比】")
    print(df2.to_string(index=False))
    print("\n")
    
    return df1, df2


def analysis_8_top_salary(conn):
    """分析8：高薪员工 TOP 10"""
    print("=" * 60)
    print("分析8：高薪员工 TOP 10")
    print("=" * 60)
    
    query = """
        SELECT 
            e.name AS 姓名,
            d.department_name AS 部门,
            p.position_name AS 职位,
            e.salary AS 薪资,
            e.performance_score AS 绩效,
            ROUND((JULIANDAY('now') - JULIANDAY(e.hire_date)) / 365.25, 1) AS 工龄
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
        INNER JOIN positions p ON e.position_id = p.position_id
        WHERE e.status = '在职'
        ORDER BY e.salary DESC
        LIMIT 10
    """
    # 逐行解释：
    # - 连接3张表：employees, departments, positions
    # - ORDER BY e.salary DESC: 按薪资降序
    # - LIMIT 10: 只取前10名
    # - 可以看出：高薪员工的特点（部门、职位、工龄、绩效）
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    print("\n")
    return df


def analysis_9_comprehensive_report(conn):
    """分析9：综合数据看板"""
    print("=" * 60)
    print("分析9：综合数据看板（关键指标）")
    print("=" * 60)
    
    query = """
        SELECT 
            '总员工数' AS 指标,
            COUNT(*) AS 数值,
            '-' AS 单位
        FROM employees
        UNION ALL
        SELECT 
            '在职人数',
            COUNT(*),
            '人'
        FROM employees WHERE status = '在职'
        UNION ALL
        SELECT 
            '离职人数',
            COUNT(*),
            '人'
        FROM employees WHERE status = '离职'
        UNION ALL
        SELECT 
            '平均薪资',
            CAST(ROUND(AVG(salary), 2) AS INTEGER),
            '元'
        FROM employees WHERE status = '在职'
        UNION ALL
        SELECT 
            '平均绩效',
            ROUND(AVG(performance_score), 2),
            '分'
        FROM employees WHERE status = '在职'
        UNION ALL
        SELECT 
            '平均工龄',
            ROUND(AVG((JULIANDAY('now') - JULIANDAY(hire_date)) / 365.25), 1),
            '年'
        FROM employees WHERE status = '在职'
    """
    # 逐行解释：
    # 1. UNION ALL:
    #    - 合并多个 SELECT 查询的结果
    #    - UNION: 合并并去重
    #    - UNION ALL: 合并不去重（性能更好）
    #    - 要求：每个 SELECT 必须有相同数量和类型的列
    
    # 2. CAST(ROUND(AVG(salary), 2) AS INTEGER):
    #    - ROUND: 先四舍五入
    #    - CAST(...AS INTEGER): 类型转换，将浮点数转为整数
    #    - 例如：12345.67 → 12346
    
    # 3. 这个查询把多个关键指标组织成一个表格
    #    - 便于一次性查看所有核心数据
    #    - 类似于数据看板的"总览"页面
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    print("\n")
    return df


# ============================================
# 第四部分：主程序
# ============================================

def main():
    """主程序：创建数据库、插入数据、执行分析"""
    print("\n")
    print("*" * 60)
    print("*" + " " * 58 + "*")
    print("*" + " " * 15 + "HR 员工数据分析系统" + " " * 15 + "*")
    print("*" + " " * 58 + "*")
    print("*" * 60)
    print("\n")
    
    # 步骤1：创建数据库
    conn = create_database()
    
    # 步骤2：插入测试数据
    insert_test_data(conn)
    
    # 步骤3：执行所有分析
    analysis_1_department_structure(conn)
    analysis_2_gender_age_distribution(conn)
    analysis_3_salary_structure(conn)
    analysis_4_work_years(conn)
    analysis_5_resignation(conn)
    analysis_6_salary_adjustment(conn)
    analysis_7_performance(conn)
    analysis_8_top_salary(conn)
    analysis_9_comprehensive_report(conn)
    
    # 关闭数据库连接
    conn.close()
    
    print("=" * 60)
    print("✓ 所有分析完成！")
    print("✓ 数据库文件：hr_analysis.db")
    print("=" * 60)
    print("\n")


if __name__ == '__main__':
    main()