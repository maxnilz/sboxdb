import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Template
from pydantic_ai import RunUsage, UsageLimits, ModelSettings, ToolDefinition
from pydantic_ai.direct import model_request_sync
from pydantic_ai.messages import (
    ModelRequest,
    SystemPromptPart,
    UserPromptPart,
    ModelResponse,
    ToolCallPart,
    TextPart,
)
from pydantic_ai.models import KnownModelName, Model, ModelRequestParameters

import templates

logger = logging.getLogger(__name__)

_template_system_prompt: Template = templates.env.get_template(
    "prompt_gen_slt_system.j2"
)
_template_user_prompt: Template = templates.env.get_template("prompt_gen_slt_user.j2")


@dataclass
class GenerateSltResponse:
    slt_script: str
    messages: list[tuple[ModelRequest, ModelResponse]]
    usage: RunUsage


class IterationLimitExceededException(Exception):
    """Exception raised when iteration limit exceeded."""

    def __init__(self, limit: int):
        self.limit = limit
        self.message = f"Iteration limit exceeded: {limit}"
        super().__init__(self.message)


class SltGenerator:
    def __init__(
        self,
        sql_spec_path: Path,
        slt_binary_path: Path,
        iteration_limit: int = 2,
        answer_dir: Path = "",
    ):
        self.sql_spec_path: Path = sql_spec_path
        self.slt_binary_path: Path = slt_binary_path
        self.iterations_limit: int = iteration_limit
        self.answer_dir: Path = answer_dir

    def generate(
        self, model: KnownModelName | Model, prompt: str
    ) -> GenerateSltResponse:
        usage = RunUsage()
        usage_limits = UsageLimits(request_limit=None)
        model_settings = ModelSettings()
        iteration_limit = 5

        answer: str | None = None
        error: str | None = None

        messages: list[tuple[ModelRequest, ModelResponse]] = []
        iteration = 0

        _continue = True
        while _continue:
            iteration += 1
            if iteration > self.iterations_limit:
                raise IterationLimitExceededException(self.iterations_limit)

            if usage_limits:
                usage_limits.check_before_request(usage)
                if usage_limits.has_token_limits():
                    usage_limits.check_tokens(usage)

            system_prompt: str = _template_system_prompt.render(
                ebnf=self.get_sql_spec()
            )
            user_prompt: str = _template_user_prompt.render(
                prompt=prompt, error=error, answer=answer
            )

            request = ModelRequest(
                parts=[
                    SystemPromptPart(content=system_prompt),
                    UserPromptPart(content=user_prompt),
                ]
            )

            model_response: ModelResponse = model_request_sync(
                model,
                messages=[request],
                model_request_parameters=ModelRequestParameters(
                    output_tools=[_answer_tool_definition()],
                    allow_text_output=False,
                ),
                model_settings=model_settings,
            )

            messages.append((request, model_response))
            for part in model_response.parts:
                match part.part_kind:
                    case "tool-call":
                        tool_call_part: ToolCallPart = part
                        args = tool_call_part.args_as_dict()
                        logger.info(f"tool call: {tool_call_part.tool_name}")
                        if tool_call_part.tool_name == _RECORD_TOOL_NAME:
                            answer = args.get("slt_script")
                            if not answer:
                                logger.error("llm did not provide a slt_script")
                                continue
                            _continue = False

                            # save the answer to a file for inspection anyway
                            self.save_answer(answer, iteration)

                            error = self.validate_slt_script(answer)
                            if error:
                                logger.error(
                                    f"asking llm to fix the invalid slt script, {error}"
                                )
                                _continue = True
                                continue
                        else:
                            logger.error(
                                f"unknown tool name: {tool_call_part.tool_name}"
                            )
                    case "text":
                        text_part: TextPart = part
                        logger.error(
                            f"unexpected textual model response: {text_part.content}"
                        )
                    case _:
                        logger.error(f"unknown model response kind: {part.part_kind}")

        assert answer is not None, "answer is unexpected missing"
        return GenerateSltResponse(slt_script=answer, messages=messages, usage=usage)

    def get_sql_spec(self) -> str:
        return open(self.sql_spec_path, "r").read()

    def save_answer(self, answer: str, iter_num: int) -> None:
        if self.answer_dir == "":
            return
        self.answer_dir.mkdir(parents=True, exist_ok=True)
        answer_path = self.answer_dir.joinpath(f"answer_{iter_num}.slt").resolve()
        with open(answer_path, "w") as f:
            f.write(answer)

    def validate_slt_script(self, script: str) -> str | None:
        logger.debug(f"validating slt script:\n {script}")
        try:
            result = subprocess.run(
                [self.slt_binary_path, "--check", "--stdin"],
                input=script.encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if result.returncode == 0:
                return None
            return result.stderr.decode() or result.stdout.decode()
        except Exception as e:
            return str(e)


_RECORD_TOOL_NAME = "record_slt_answer"


def _answer_tool_definition() -> ToolDefinition:
    return ToolDefinition(
        name=_RECORD_TOOL_NAME,
        description="Provide a SQL Logical Test script that addresses the user's question.",
        parameters_json_schema={
            "type": "object",
            "properties": {
                "slt_script": {
                    "type": "string",
                    "description": (
                        "A valid SQL Logical Test script that accurately addresses the user's prompt"
                    ),
                },
            },
            "required": [
                "slt_script",
            ],
        },
    )
