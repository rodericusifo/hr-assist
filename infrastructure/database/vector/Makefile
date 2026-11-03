SHELL := /bin/bash
PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))../../..)
include $(PROJECT_ROOT)/colors.mk

# ENV VARIABLE
AVAILABLE_VECTOR := qdrant

.PHONY: check-vector
check-vector:
	@$(PRINTF_CMD) "$(call log_action,Check App Vector Database ($(NAME) | $(ENV)))"
	@for vector in $(VECTOR); do \
		if [[ ! " $(AVAILABLE_VECTOR) " =~ " $${vector} " ]]; then \
			$(PRINTF_CMD) "$(RED)ERROR: Vector database [$${vector}] is not available. Available Vector databases are: $(AVAILABLE_VECTOR)$(NOCOLOR)"; \
			exit 1; \
		fi; \
	done
	@$(PRINTF_CMD) "$(call log_action_end,All Vector Databases are available)"

.PHONY: run-vector
run-vector: check-vector
	@$(PRINTF_CMD) "$(call log_action,Start App Vector Database ($(NAME) | $(ENV)))"
	@for vector in $(VECTOR); do \
		$(PRINTF_CMD) "$(call log_action,Generate App Vector Database Volumes [$${vector}] ($(NAME) | $(ENV)))"; \
		for node in 0 1 2; do \
			docker volume ls | grep $(SLUG)_qdrant-$${node}_data_$(ENV) || docker volume create --name $(SLUG)_qdrant-$${node}_data_$(ENV); \
		done; \
		$(PRINTF_CMD) "$(call log_action,Start App Vector Database [$${vector}] ($(NAME) | $(ENV)))"; \
		ENV=$(ENV) SLUG=$(SLUG) docker compose -p $(NAME)_vector --env-file $${vector}/env/$(ENV).env -f $${vector}/docker-compose.yaml up --build -d; \
		if [ "$${vector}" = "qdrant" ]; then \
			ready=0; \
			for attempt in $$(seq 1 60); do \
				if curl -fsS --retry 2 --retry-connrefused --retry-delay 1 http://localhost:6339/healthz >/dev/null 2>&1; then \
					ready=1; \
					break; \
				fi; \
				sleep 1; \
			done; \
			if [ "$${ready}" -ne 1 ]; then \
				$(PRINTF_CMD) "$(RED)ERROR: Qdrant router did not become ready$(NOCOLOR)"; \
				exit 1; \
			fi; \
			if ! curl -fsS --retry 3 --retry-delay 1 http://localhost:6339/collections/resumes >/dev/null 2>&1; then \
				curl -fsS --retry 3 --retry-delay 1 -X PUT http://localhost:6339/collections/resumes \
					-H 'Content-Type: application/json' \
					-d '{"vectors":{"size":1536,"distance":"Cosine"},"shard_number":3,"replication_factor":2,"write_consistency_factor":1}' >/dev/null; \
			fi; \
			curl -fsS --retry 3 --retry-delay 1 http://localhost:6339/collections/resumes >/dev/null; \
		fi; \
	done
	@$(PRINTF_CMD) "$(call log_action_end,Vector Databases started successfully)"

.PHONY: stop-vector
stop-vector: check-vector
	@$(PRINTF_CMD) "$(call log_action,Stop App Vector Database ($(NAME) | $(ENV)))"
	@for vector in $(VECTOR); do \
		$(PRINTF_CMD) "$(call log_action,Stop App Vector Database [$${vector}] ($(NAME) | $(ENV)))"; \
		ENV=$(ENV) SLUG=$(SLUG) docker compose -p $(NAME)_vector --env-file $${vector}/env/$(ENV).env -f $${vector}/docker-compose.yaml down; \
	done
	@$(PRINTF_CMD) "$(call log_action_end,Vector Databases stopped successfully)"
