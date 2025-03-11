class QueryEngine(NLSQLTableQueryEngine):
    """Specialized engine that translates natural language to SQL for soccer data."""

    def __init__(self, sql_database, llm, always_infer=True, **kwargs):
        """
        Initialize with SQL database and LLM.

        Args:
            sql_database: SQL database connection
            llm: Language model for query generation
            always_infer: Always use inference for SQL generation
            **kwargs: Additional arguments passed to parent class
        """
        super().__init__(sql_database=sql_database, **kwargs)
        self.sql_database = sql_database
        self.llm = llm
        self.memory_context = None  # Initialize memory context attribute
        self.always_infer = always_infer  # Flag to force using dynamic inference for all queries
        self.use_fuzzy_team_matching = True  # Enable advanced team name matching
        print(f"Query engine initialized with always_infer={self.always_infer}")

        # Load teams for context
        self.teams = get_all_teams(self.sql_database)
        print(f"📊 Loaded {len(self.teams)} teams")